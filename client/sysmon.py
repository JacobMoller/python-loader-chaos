#!/usr/bin/env python3
"""
sysmon.py — Real-time system resource monitor with command benchmarking.

Launches a command (subprocess), monitors CPU/RAM/GPU in real time during
and after execution, computes rolling + global averages, tracks peaks,
and prints a final summary on exit.

The monitoring does NOT stop when the command finishes — it keeps running
so you can observe the cooldown / aftermath. Press Ctrl+C to stop and
get the full report.

Dependencies:
    pip install psutil GPUtil

Usage:
    python3 sysmon.py -c "python3 my_pipeline.py --items 2000"
    python3 sysmon.py -c "./process_lsc.sh" -w 120 -i 0.5
    python3 sysmon.py -c "echo placeholder_command" --post-delay 30
"""

import sys
import time
import signal
import argparse
import subprocess
import threading
from collections import deque
from datetime import datetime, timedelta

try:
    import psutil
except ImportError:
    print("ERROR: psutil is required. Install it with:")
    print("  pip install psutil")
    sys.exit(1)

try:
    import GPUtil
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False


# ── ANSI helpers ──────────────────────────────────────────────────────
class Colors:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    BG_DARK = "\033[48;5;235m"
    CLEAR   = "\033[2J\033[H"


def color_by_usage(value: float) -> str:
    if value < 40:
        return Colors.GREEN
    elif value < 70:
        return Colors.YELLOW
    elif value < 90:
        return Colors.RED
    else:
        return Colors.BOLD + Colors.RED


def bar(value: float, width: int = 30) -> str:
    filled = int(value / 100 * width)
    empty = width - filled
    c = color_by_usage(value)
    return f"{c}{'█' * filled}{Colors.DIM}{'░' * empty}{Colors.RESET}"


def format_bytes(b: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def format_duration(td: timedelta) -> str:
    total = int(td.total_seconds())
    h, r = divmod(total, 3600)
    m, s = divmod(r, 60)
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    elif m > 0:
        return f"{m}m{s:02d}s"
    else:
        return f"{s}s"


# ── Data collection ───────────────────────────────────────────────────
def sample() -> dict:
    cpu_percent = psutil.cpu_percent(interval=None)
    cpu_freq = psutil.cpu_freq()
    mem = psutil.virtual_memory()

    data = {
        "timestamp": datetime.now(),
        "cpu_percent": cpu_percent,
        "cpu_freq_mhz": cpu_freq.current if cpu_freq else 0.0,
        "cpu_cores": psutil.cpu_count(logical=True),
        "ram_percent": mem.percent,
        "ram_used": mem.used,
        "ram_total": mem.total,
        "gpu": [],
    }

    if GPU_AVAILABLE:
        try:
            gpus = GPUtil.getGPUs()
            for gpu in gpus:
                data["gpu"].append({
                    "id": gpu.id,
                    "name": gpu.name,
                    "load": gpu.load * 100,
                    "memory_used": gpu.memoryUsed,
                    "memory_total": gpu.memoryTotal,
                    "temperature": gpu.temperature,
                })
        except Exception:
            pass

    return data


# ── Stats tracker ─────────────────────────────────────────────────────
class StatsTracker:
    """Accumulates stats across the entire run for the final summary."""

    def __init__(self):
        self.start_time: datetime = datetime.now()
        self.cmd_end_time = None
        self.total_samples = 0

        # Global
        self.cpu_sum = 0.0
        self.cpu_peak = 0.0
        self.cpu_peak_at = None
        self.ram_sum = 0.0
        self.ram_peak = 0.0
        self.ram_peak_at = None
        self.ram_peak_bytes = 0

        # GPU global
        self.gpu_load_sum: dict = {}
        self.gpu_load_peak: dict = {}
        self.gpu_mem_peak: dict = {}
        self.gpu_temp_peak: dict = {}
        self.gpu_names: dict = {}

        # During-command subset
        self.cmd_samples = 0
        self.cmd_cpu_sum = 0.0
        self.cmd_cpu_peak = 0.0
        self.cmd_ram_sum = 0.0
        self.cmd_ram_peak = 0.0
        self.cmd_gpu_load_sum: dict = {}
        self.cmd_gpu_load_peak: dict = {}

    def record(self, s: dict, cmd_running: bool):
        self.total_samples += 1
        ts = s["timestamp"]

        cpu = s["cpu_percent"]
        self.cpu_sum += cpu
        if cpu > self.cpu_peak:
            self.cpu_peak = cpu
            self.cpu_peak_at = ts

        ram = s["ram_percent"]
        self.ram_sum += ram
        if ram > self.ram_peak:
            self.ram_peak = ram
            self.ram_peak_at = ts
            self.ram_peak_bytes = s["ram_used"]

        for gpu in s.get("gpu", []):
            gid = gpu["id"]
            self.gpu_names[gid] = gpu["name"]
            self.gpu_load_sum[gid] = self.gpu_load_sum.get(gid, 0.0) + gpu["load"]
            self.gpu_load_peak[gid] = max(self.gpu_load_peak.get(gid, 0.0), gpu["load"])
            self.gpu_mem_peak[gid] = max(self.gpu_mem_peak.get(gid, 0.0), gpu["memory_used"])
            self.gpu_temp_peak[gid] = max(self.gpu_temp_peak.get(gid, 0.0), gpu["temperature"])

        if cmd_running:
            self.cmd_samples += 1
            self.cmd_cpu_sum += cpu
            self.cmd_cpu_peak = max(self.cmd_cpu_peak, cpu)
            self.cmd_ram_sum += ram
            self.cmd_ram_peak = max(self.cmd_ram_peak, ram)
            for gpu in s.get("gpu", []):
                gid = gpu["id"]
                self.cmd_gpu_load_sum[gid] = self.cmd_gpu_load_sum.get(gid, 0.0) + gpu["load"]
                self.cmd_gpu_load_peak[gid] = max(self.cmd_gpu_load_peak.get(gid, 0.0), gpu["load"])


# ── Display ───────────────────────────────────────────────────────────
def render(current: dict, history: deque, window: int,
           cmd_status: str, cmd_str: str, cmd_retcode,
           tracker: StatsTracker):

    n = len(history)
    avg_cpu = sum(s["cpu_percent"] for s in history) / n
    avg_ram = sum(s["ram_percent"] for s in history) / n

    gpu_avgs = {}
    if current["gpu"]:
        for gid_info in current["gpu"]:
            gid = gid_info["id"]
            loads = [s["gpu"][i]["load"]
                     for s in history
                     for i, g in enumerate(s.get("gpu", []))
                     if g["id"] == gid]
            mems = [s["gpu"][i]["memory_used"]
                    for s in history
                    for i, g in enumerate(s.get("gpu", []))
                    if g["id"] == gid]
            if loads:
                gpu_avgs[gid] = {
                    "load": sum(loads) / len(loads),
                    "mem": sum(mems) / len(mems),
                }

    ts = current["timestamp"].strftime("%H:%M:%S")
    elapsed = current["timestamp"] - tracker.start_time
    print(Colors.CLEAR, end="")

    W = 66
    print(f"{Colors.BOLD}{Colors.CYAN}╔{'═' * W}╗{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}║  ⚡ SYSMON — Benchmark Monitor{' ' * (W - 32)}║{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}╠{'═' * W}╣{Colors.RESET}")

    # Command status
    if cmd_status == "RUNNING":
        status_color = Colors.YELLOW
        status_icon = "⏳"
        status_text = "RUNNING"
    elif cmd_status == "COMPLETED":
        if cmd_retcode == 0:
            status_color = Colors.GREEN
            status_icon = "✅"
            status_text = f"COMPLETED (exit 0)"
        else:
            status_color = Colors.RED
            status_icon = "❌"
            status_text = f"FAILED (exit {cmd_retcode})"
    else:
        status_color = Colors.DIM
        status_icon = "⏸"
        status_text = "WAITING"

    cmd_display = cmd_str if len(cmd_str) <= 50 else cmd_str[:47] + "..."
    print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}Cmd:{Colors.RESET}    {cmd_display}")
    print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}Status:{Colors.RESET} {status_color}{status_icon} {status_text}{Colors.RESET}")

    if cmd_status == "RUNNING":
        cmd_dur = current["timestamp"] - tracker.start_time
        print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}Cmd time:{Colors.RESET} {format_duration(cmd_dur)} {Colors.DIM}(running){Colors.RESET}")
    elif tracker.cmd_end_time:
        cmd_dur = tracker.cmd_end_time - tracker.start_time
        post_dur = current["timestamp"] - tracker.cmd_end_time
        print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}Cmd time:{Colors.RESET} {format_duration(cmd_dur)}   {Colors.DIM}Post-cmd: +{format_duration(post_dur)}{Colors.RESET}")

    print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.DIM}Elapsed: {format_duration(elapsed)}   Samples: {n}/{window}   Total: {tracker.total_samples}{Colors.RESET}")
    print(f"{Colors.CYAN}╠{'═' * W}╣{Colors.RESET}")

    # CPU
    cpu_now = current["cpu_percent"]
    freq = current["cpu_freq_mhz"]
    cores = current["cpu_cores"]
    global_avg_cpu = tracker.cpu_sum / tracker.total_samples if tracker.total_samples else 0
    print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}CPU{Colors.RESET}  ({cores} threads, {freq:.0f} MHz)")
    print(f"{Colors.CYAN}║{Colors.RESET}    Now:      {bar(cpu_now)}  {color_by_usage(cpu_now)}{cpu_now:5.1f}%{Colors.RESET}")
    print(f"{Colors.CYAN}║{Colors.RESET}    Roll avg: {bar(avg_cpu)}  {color_by_usage(avg_cpu)}{avg_cpu:5.1f}%{Colors.RESET}")
    print(f"{Colors.CYAN}║{Colors.RESET}    {Colors.DIM}Global avg: {color_by_usage(global_avg_cpu)}{global_avg_cpu:5.1f}%{Colors.RESET}   {Colors.DIM}Peak: {color_by_usage(tracker.cpu_peak)}{tracker.cpu_peak:5.1f}%{Colors.RESET}")
    print(f"{Colors.CYAN}║{Colors.RESET}")

    # RAM
    ram_now = current["ram_percent"]
    ram_used = current["ram_used"]
    ram_total = current["ram_total"]
    global_avg_ram = tracker.ram_sum / tracker.total_samples if tracker.total_samples else 0
    print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}RAM{Colors.RESET}  ({format_bytes(ram_used)} / {format_bytes(ram_total)})")
    print(f"{Colors.CYAN}║{Colors.RESET}    Now:      {bar(ram_now)}  {color_by_usage(ram_now)}{ram_now:5.1f}%{Colors.RESET}")
    print(f"{Colors.CYAN}║{Colors.RESET}    Roll avg: {bar(avg_ram)}  {color_by_usage(avg_ram)}{avg_ram:5.1f}%{Colors.RESET}")
    print(f"{Colors.CYAN}║{Colors.RESET}    {Colors.DIM}Global avg: {color_by_usage(global_avg_ram)}{global_avg_ram:5.1f}%{Colors.RESET}   {Colors.DIM}Peak: {color_by_usage(tracker.ram_peak)}{tracker.ram_peak:5.1f}% ({format_bytes(tracker.ram_peak_bytes)}){Colors.RESET}")

    # GPU(s)
    if current["gpu"]:
        for gpu in current["gpu"]:
            gid = gpu["id"]
            load = gpu["load"]
            mem_u = gpu["memory_used"]
            mem_t = gpu["memory_total"]
            temp = gpu["temperature"]

            avg_load = gpu_avgs.get(gid, {}).get("load", load)
            global_avg_load = (tracker.gpu_load_sum.get(gid, 0) / tracker.total_samples) if tracker.total_samples else 0
            peak_load = tracker.gpu_load_peak.get(gid, 0)
            peak_mem = tracker.gpu_mem_peak.get(gid, 0)
            peak_temp = tracker.gpu_temp_peak.get(gid, 0)

            print(f"{Colors.CYAN}║{Colors.RESET}")
            print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.BOLD}GPU {gid}{Colors.RESET}  {gpu['name']}  ({temp}°C)")
            print(f"{Colors.CYAN}║{Colors.RESET}    Load Now:  {bar(load)}  {color_by_usage(load)}{load:5.1f}%{Colors.RESET}")
            print(f"{Colors.CYAN}║{Colors.RESET}    Roll avg:  {bar(avg_load)}  {color_by_usage(avg_load)}{avg_load:5.1f}%{Colors.RESET}")
            print(f"{Colors.CYAN}║{Colors.RESET}    {Colors.DIM}Global avg: {global_avg_load:5.1f}%   Peak: {peak_load:5.1f}%{Colors.RESET}")
            print(f"{Colors.CYAN}║{Colors.RESET}    {Colors.DIM}VRAM: {format_bytes(mem_u*1024*1024)} / {format_bytes(mem_t*1024*1024)}  Peak: {format_bytes(peak_mem*1024*1024)}  Temp peak: {peak_temp}°C{Colors.RESET}")
    elif not GPU_AVAILABLE:
        print(f"{Colors.CYAN}║{Colors.RESET}")
        print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.DIM}GPU: GPUtil not installed or no NVIDIA GPU detected{Colors.RESET}")

    print(f"{Colors.CYAN}╠{'═' * W}╣{Colors.RESET}")
    if cmd_status == "COMPLETED":
        print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.GREEN}Command finished — monitoring continues{Colors.RESET}  {Colors.DIM}Ctrl+C → summary{Colors.RESET}")
    else:
        print(f"{Colors.CYAN}║{Colors.RESET}  {Colors.DIM}Ctrl+C → stop & print summary{Colors.RESET}")
    print(f"{Colors.CYAN}╚{'═' * W}╝{Colors.RESET}")


# ── Final summary ─────────────────────────────────────────────────────
def print_summary(tracker: StatsTracker, cmd_str: str, cmd_retcode,
                  cmd_started_at=None, cmd_ended_at=None, stop_time=None):
    end_time = stop_time or datetime.now()
    total_dur = end_time - tracker.start_time
    n = tracker.total_samples

    if n == 0:
        print(f"\n{Colors.DIM}No samples collected.{Colors.RESET}")
        return

    global_avg_cpu = tracker.cpu_sum / n
    global_avg_ram = tracker.ram_sum / n

    TS_FMT = "%Y-%m-%d %H:%M:%S"

    W = 66
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * W}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  📊 BENCHMARK SUMMARY{Colors.RESET}")
    print(f"{Colors.CYAN}{'═' * W}{Colors.RESET}")
    print()

    cmd_display = cmd_str if len(cmd_str) <= 55 else cmd_str[:52] + "..."
    print(f"  {Colors.BOLD}Command:{Colors.RESET}     {cmd_display}")
    if cmd_retcode is not None:
        rc_color = Colors.GREEN if cmd_retcode == 0 else Colors.RED
        print(f"  {Colors.BOLD}Exit code:{Colors.RESET}   {rc_color}{cmd_retcode}{Colors.RESET}")
    print()

    # ── Exact timestamps ──
    print(f"  {Colors.CYAN}── Timestamps ──{Colors.RESET}")
    if cmd_started_at:
        print(f"    Command started:  {Colors.BOLD}{cmd_started_at.strftime(TS_FMT)}{Colors.RESET}")
    if cmd_ended_at:
        print(f"    Command ended:    {Colors.BOLD}{cmd_ended_at.strftime(TS_FMT)}{Colors.RESET}")
    else:
        print(f"    Command ended:    {Colors.YELLOW}still running at stop{Colors.RESET}")
    print(f"    Monitoring stopped: {Colors.BOLD}{end_time.strftime(TS_FMT)}{Colors.RESET}")
    print()

    # ── Durations ──
    print(f"  {Colors.BOLD}Total monitoring:{Colors.RESET}  {format_duration(total_dur)}")
    if tracker.cmd_end_time:
        cmd_dur = tracker.cmd_end_time - tracker.start_time
        post_dur = end_time - tracker.cmd_end_time
        print(f"  {Colors.BOLD}Command duration:{Colors.RESET}  {format_duration(cmd_dur)}")
        print(f"  {Colors.BOLD}Post-command:{Colors.RESET}      {format_duration(post_dur)}")
    print(f"  {Colors.BOLD}Samples:{Colors.RESET}           {n}")
    print()

    # ── Overall ──
    print(f"  {Colors.CYAN}── Overall (full monitoring window) ──{Colors.RESET}")
    peak_cpu_ts = f"  {Colors.DIM}@ {tracker.cpu_peak_at.strftime('%H:%M:%S')}{Colors.RESET}" if tracker.cpu_peak_at else ""
    peak_ram_ts = f"  {Colors.DIM}@ {tracker.ram_peak_at.strftime('%H:%M:%S')}{Colors.RESET}" if tracker.ram_peak_at else ""
    print(f"    CPU  avg: {color_by_usage(global_avg_cpu)}{global_avg_cpu:5.1f}%{Colors.RESET}    peak: {color_by_usage(tracker.cpu_peak)}{tracker.cpu_peak:5.1f}%{Colors.RESET}{peak_cpu_ts}")
    print(f"    RAM  avg: {color_by_usage(global_avg_ram)}{global_avg_ram:5.1f}%{Colors.RESET}    peak: {color_by_usage(tracker.ram_peak)}{tracker.ram_peak:5.1f}%{Colors.RESET} ({format_bytes(tracker.ram_peak_bytes)}){peak_ram_ts}")

    for gid in sorted(tracker.gpu_names):
        avg_g = tracker.gpu_load_sum.get(gid, 0) / n
        peak_g = tracker.gpu_load_peak.get(gid, 0)
        peak_m = tracker.gpu_mem_peak.get(gid, 0)
        peak_t = tracker.gpu_temp_peak.get(gid, 0)
        print(f"    GPU {gid} ({tracker.gpu_names[gid]})")
        print(f"         load avg: {avg_g:5.1f}%   peak: {peak_g:5.1f}%")
        print(f"         VRAM peak: {format_bytes(peak_m*1024*1024)}   Temp peak: {peak_t}°C")

    # ── During command only ──
    if tracker.cmd_samples > 0:
        print()
        print(f"  {Colors.CYAN}── During command execution ({tracker.cmd_samples} samples) ──{Colors.RESET}")
        cmd_avg_cpu = tracker.cmd_cpu_sum / tracker.cmd_samples
        cmd_avg_ram = tracker.cmd_ram_sum / tracker.cmd_samples
        print(f"    CPU  avg: {color_by_usage(cmd_avg_cpu)}{cmd_avg_cpu:5.1f}%{Colors.RESET}    peak: {color_by_usage(tracker.cmd_cpu_peak)}{tracker.cmd_cpu_peak:5.1f}%{Colors.RESET}")
        print(f"    RAM  avg: {color_by_usage(cmd_avg_ram)}{cmd_avg_ram:5.1f}%{Colors.RESET}    peak: {color_by_usage(tracker.cmd_ram_peak)}{tracker.cmd_ram_peak:5.1f}%{Colors.RESET}")
        for gid in sorted(tracker.gpu_names):
            if gid in tracker.cmd_gpu_load_sum:
                avg_g = tracker.cmd_gpu_load_sum[gid] / tracker.cmd_samples
                peak_g = tracker.cmd_gpu_load_peak.get(gid, 0)
                print(f"    GPU {gid}  load avg: {avg_g:5.1f}%   peak: {peak_g:5.1f}%")

    print()
    print(f"{Colors.CYAN}{'═' * W}{Colors.RESET}")


# ── Subprocess runner (threaded) ──────────────────────────────────────
class CommandRunner:
    def __init__(self, cmd: str):
        self.cmd = cmd
        self.process = None
        self.returncode = None
        self.running = False
        self.finished = False
        self.thread = None
        self.started_at = None
        self.ended_at = None

    def start(self):
        self.running = True
        self.started_at = datetime.now()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            self.process = subprocess.Popen(
                self.cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            self.process.wait()
            self.returncode = self.process.returncode
        except Exception:
            self.returncode = -1
        finally:
            self.running = False
            self.finished = True
            self.ended_at = datetime.now()


# ── Main ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="System resource monitor with command benchmarking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 sysmon.py -c "python3 pipeline.py --items 2000"
  python3 sysmon.py -c "./run_lsc_processing.sh" -w 120
  python3 sysmon.py -c "sleep 10"
        """,
    )
    parser.add_argument("-c", "--command", type=str, required=True,
                        help="Command to execute and monitor")
    parser.add_argument("-w", "--window", type=int, default=60,
                        help="Rolling average window in samples (default: 60)")
    parser.add_argument("-i", "--interval", type=float, default=1.0,
                        help="Sampling interval in seconds (default: 1.0)")
    args = parser.parse_args()

    history: deque = deque(maxlen=args.window)
    tracker = StatsTracker()

    # Prime psutil
    psutil.cpu_percent(interval=None)

    # Launch command in background
    runner = CommandRunner(args.command)
    runner.start()

    def handle_sigint(_sig, _frame):
        stop_time = datetime.now()
        tracker.cmd_end_time = tracker.cmd_end_time or runner.ended_at
        print_summary(tracker, args.command, runner.returncode,
                      cmd_started_at=runner.started_at,
                      cmd_ended_at=runner.ended_at,
                      stop_time=stop_time)
        if runner.process and runner.running:
            try:
                runner.process.terminate()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    # ── Monitor loop — runs forever until Ctrl+C ──
    while True:
        s = sample()
        cmd_running = runner.running

        if runner.finished and tracker.cmd_end_time is None:
            tracker.cmd_end_time = runner.ended_at

        tracker.record(s, cmd_running)
        history.append(s)

        if runner.running:
            cmd_status = "RUNNING"
        elif runner.finished:
            cmd_status = "COMPLETED"
        else:
            cmd_status = "WAITING"

        render(s, history, args.window,
               cmd_status, args.command, runner.returncode, tracker)

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
