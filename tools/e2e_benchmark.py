import argparse
import base64
import csv
import json
import re
import shutil
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path

DB = "db"
CLIENT = "python-client"

# 15672 is the management API for rabbitMQ, see docker-compose
RABBIT = "http://localhost:15672"

# default rabbitmq user, needed to log in to the management API
AUTH = "Basic " + base64.b64encode(b"guest:guest").decode()
# tables the plugins write to, the last write to one of them is the end of the run
TABLES = ["medias", "taggings", "tags", "nodes"]

# nvidia-smi is only there on machines with an NVIDIA GPU
HAS_GPU = shutil.which("nvidia-smi") is not None

UNITS = {"b": 1, "kib": 2**10, "mib": 2**20, "gib": 2**30, "kb": 1e3, "mb": 1e6, "gb": 1e9}


# Run a command and return its output
def run(command, input_text=None):
    result = subprocess.run(command, input=input_text, capture_output=True, text=True,
                            encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise RuntimeError(f"{' '.join(command)} failed: {result.stderr.strip()}")
    return result.stdout

# Run a query in the db container. -At removes formatting and headers, 
# -F, separates fields with commas
def sql(query):
    return run(["docker", "exec", DB, "psql", "-U", "postgres", "-d", "loader-testing",
                "-At", "-F,", "-c", query]).strip()

# retrieve total rows inserted/updated/deleted in all tables. 
# Postgres keeps these counters so this is cheap
def get_db_writes():
    return int(sql("SELECT sum(n_tup_ins + n_tup_upd + n_tup_del) FROM pg_stat_user_tables") or 0)

# cpu and memory of every container right now -> list of [container, cpu %, memory MiB] (100% cpu = one core)
def docker_stats():
    output = run(["docker", "stats", "--no-stream", "--format", "{{.Name}},{{.CPUPerc}},{{.MemUsage}}"])
    stats = []
    for line in output.splitlines():
        name, cpu, mem = line.split(",")

        if cpu.startswith("--"):  # container is starting
            continue

        m = re.search(r"([\d.]+)\s*([KMG]?i?B)", mem, re.IGNORECASE)
        stats.append([name, float(cpu.rstrip("%")), float(m.group(1)) * UNITS[m.group(2).lower()] / 2**20]) #2**20 converts bytes into MiB 
    return stats

# utilization and memory of every GPU right now -> list of [gpu index, utilization %, memory MiB]
def gpu_stats():
    output = run(["nvidia-smi", "--query-gpu=index,utilization.gpu,memory.used", "--format=csv,noheader,nounits"])
    return [line.split(", ") for line in output.splitlines()]

# size of every rabbitmq queue right now -> list of [queue, ready, unacked, consumers]
# ready = waiting to be delivered, unacked = delivered to a plugin but not acknowledged yet.
# RabbitMQ refreshes these numbers every ~5 s
def queue_sizes():
    url = RABBIT + "/api/queues?columns=name,messages_ready,messages_unacknowledged,consumers"
    request = urllib.request.Request(url, headers={"Authorization": AUTH})
    with urllib.request.urlopen(request, timeout=10) as response:
        queues = json.load(response)
    return [[q["name"], q.get("messages_ready", 0), q.get("messages_unacknowledged", 0), q.get("consumers", 0)]
            for q in queues]

def reset_and_load_tagsets():
    # reset db using the client (it asks for confirmation)
    run(["docker", "exec", "-i", CLIENT, "python", "cli.py", "reset"], input_text="y\n")

    # copy the tagset file to the client container and load it
    # TODO pick correct tagset for the benchmark
    run(["docker", "cp", "client/LscTagsetFile.csv", f"{CLIENT}:/app/tagsets.csv"])
    run(["docker", "exec", CLIENT, "python", "cli.py", "import_tagsets", "--format", "csv", "tagsets.csv"])

    # exif needs Location to save GPS, which in turn triggers geodatas
    # TODO pick correct tagset for benchnark
    run(["docker", "exec", CLIENT, "python", "cli.py", "add", "tagset", "Location", "1"])

# wait until the events from the reset and tagset import are processed, so they aren't measured
def wait_until_idle(stable_for=10):
    last_writes = None
    stable_since = time.time()
    while time.time() - stable_since < stable_for:
        writes = get_db_writes()
        if writes != last_writes:
            last_writes = writes
            stable_since = time.time()
        time.sleep(1)

# Add the medias in `folder`, 
# wait until the pipeline is done and save the results in run_dir.
def measure(folder, run_dir, poll, quiet):
    run_dir.mkdir(parents=True)

    # cpu and memory per container
    stats_file = open(run_dir / "stats.csv", "w", newline="", encoding="utf-8")
    stats_csv = csv.writer(stats_file)
    stats_csv.writerow(["t_s", "container", "cpu_percent", "mem_mib"])

    # gpu utilization and gpu memory (stays empty without an NVIDIA GPU)
    gpu_file = open(run_dir / "gpu.csv", "w", newline="", encoding="utf-8")
    gpu_csv = csv.writer(gpu_file)
    gpu_csv.writerow(["t_s", "gpu", "util_percent", "mem_mib"])

    # messages per rabbitmq queue
    queues_file = open(run_dir / "queues.csv", "w", newline="", encoding="utf-8")
    queues_csv = csv.writer(queues_file)
    queues_csv.writerow(["t_s", "queue", "ready", "unacked", "consumers"])

    start = float(sql("SELECT extract(epoch FROM now())"))  # db clock, using the commit timestamps TODO maybe ask if this is okay
    start_of_measure = time.time()
    
    # start the insert without waiting for it
    process = subprocess.Popen(["docker", "exec", CLIENT, "python", "cli.py", "add", "medias", folder],
                               stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

    last_writes = None
    last_change = start_of_measure
    while True:
        now = time.time()
        time_since_start = round(now - start_of_measure, 1)

        writes = get_db_writes()
        if writes != last_writes: # track when last write happend
            last_writes = writes
            last_change = now

        for name, cpu, mem in docker_stats():
            stats_csv.writerow([time_since_start, name, cpu, mem])

        if HAS_GPU:
            for index, util, mem in gpu_stats():
                gpu_csv.writerow([time_since_start, index, util, mem])

        for queue, ready, unacked, consumers in queue_sizes():
            queues_csv.writerow([time_since_start, queue, ready, unacked, consumers])

        stats_file.flush()
        gpu_file.flush()
        queues_file.flush()

        insert_running = process.poll() is None  # poll() is None while the process is running
        quiet_for = now - last_change
        print(f"time: {time_since_start:.0f}s db writes {writes} quiet for {quiet_for:.0f}s")
        
        if not insert_running and quiet_for >= quiet:
            break

        time.sleep(max(0, poll - (time.time() - now)))

    stats_file.close()
    gpu_file.close()
    queues_file.close()

    # get last write from db
    last_write = " UNION ALL ".join(f"SELECT max(pg_xact_commit_timestamp(xmin)) AS ts FROM {t}" for t in TABLES)
    media_count, end = sql(f"SELECT (SELECT count(*) FROM medias), "
                           f"extract(epoch FROM (SELECT max(ts) FROM ({last_write}) w))").split(",")

    result = {
        "media_count": int(media_count),
        "total_s": round(float(end) - start, 2) if end else None,
    }
    with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    return result


def main():
    parser = argparse.ArgumentParser(description="Time how long the pipeline takes to process a folder of medias")
    parser.add_argument("--folder", required=True, help="media folder inside the containers, e.g. /app/medias/bench_1000")
    parser.add_argument("--runs", type=int, default=1, help="number of runs")
    parser.add_argument("--label", default="run", help="name of the experiment, used in the output folder")
    # docker stats --no-stream needs about 2s per call (it measures cpu between two reads),
    # so with 1 s the samples run back to back and cover the whole run
    parser.add_argument("--poll", type=float, default=1, help="seconds between samples (in practice ~2 s)")
    parser.add_argument("--quiet", type=float, default=60, help="seconds without DB writes before a run is done")
    args = parser.parse_args()

    out_dir = Path("bench_results") / f"{args.label}_{datetime.now():%Y%m%d_%H%M%S}"
    results = []
    for i in range(1, args.runs + 1):
        print(f"=== run {i}/{args.runs} ===")
        reset_and_load_tagsets()
        wait_until_idle()
        result = measure(args.folder, out_dir / f"run_{i:02d}", args.poll, args.quiet)
        print(f"medias {result['media_count']}, total {result['total_s']}s")
        results.append(result)

    # one row per run
    with open(out_dir / "runs.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["run", "media_count", "total_s"])
        for i, result in enumerate(results, start=1):
            writer.writerow([i, result["media_count"], result["total_s"]])
    print(f"Results in {out_dir}")

if __name__ == "__main__":
    main()
