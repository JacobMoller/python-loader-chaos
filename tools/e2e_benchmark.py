import argparse
import base64
import csv
import json
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DB = "db"
CLIENT = "python-client"
# face recognition container in docker-compose.yaml and in docker-compose.standalone.yaml
FACES = ["plugin-face-recognition", "standalone-face-recognition"]
# logged by face recognition once it consumes again (rabbitMQ_helpers.listen / rabbitMQ_standalone.consume)
FACES_READY = ["Listening on", "Consuming from"]
# where face recognition saves every face it has seen, see the known_faces volume in docker-compose
KNOWN_FACES = "/app/known_faces"

# 15672 is the management API for rabbitMQ, see docker-compose
RABBIT = "http://localhost:15672"

# default rabbitmq user, needed to log in to the management API
AUTH = "Basic " + base64.b64encode(b"guest:guest").decode()
# tables the plugins write to, the last write to one of them is the end of the run
TABLES = ["medias", "taggings", "tags", "nodes"]

# nvidia-smi is only there on machines with an NVIDIA GPU
HAS_GPU = shutil.which("nvidia-smi") is not None

UNITS = {"b": 1, "kib": 2**10, "mib": 2**20, "gib": 2**30, "kb": 1e3, "mb": 1e6, "gb": 1e9}

# the plugins log "TIMING received ID=<media>", "TIMING start ID=<media>" and
# "TIMING done ID=<media> status=<ok|error|skipped|timeout>"
TIMING = re.compile(r"TIMING (received|start|done) ID=(\S+)(?: status=(\S+))?")
# the orchestrator logs "QUEUE size=<n>" whenever its internal media queue grows or shrinks
QUEUE = re.compile(r"QUEUE size=(\d+)")
ORCHESTRATOR = "media-orchestrator"


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

# the same counters per table -> list of [table, inserted, updated, deleted]
def db_writes_per_table():
    output = sql("SELECT relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables ORDER BY relname")
    return [[name, int(ins), int(upd), int(dele)]
            for name, ins, upd, dele in (line.split(",") for line in output.splitlines())]

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

# cgroup counters of every running container -> {container: {counter: microseconds}}
# cpu.stat usage_usec is the exact cpu time the container has used (user_usec + system_usec),
# memory.pressure total is how long its processes were stalled waiting for memory
# ("some" = at least one process, "full" = all of them). Both count from when the container started
def cgroup_counters():
    counters = {}
    for name in run(["docker", "ps", "--format", "{{.Names}}"]).split():
        output = run(["docker", "exec", name, "cat", "/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.pressure"])
        values = {}
        for line in output.splitlines():
            key, rest = line.split(" ", 1)
            if key in ("some", "full"):  # some avg10=0.00 avg60=0.00 avg300=0.00 total=0
                values[f"mem_{key}_usec"] = int(rest.rsplit("total=", 1)[1])
            else:
                values[key] = int(rest)
        counters[name] = values
    return counters

# docker logs --timestamps uses RFC3339Nano (2026-11-04T10:00:00.123456789Z, trailing zeros dropped) -> unix seconds
def docker_time(stamp):
    whole, _, fraction = stamp.rstrip("Z").partition(".")
    seconds = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    return seconds + float("0." + (fraction or "0"))

# when every container received, started and finished each media since `since` (unix seconds), from its
# TIMING lines -> list of [container, media id, received, start, end, status], and the orchestrator's
# queue size -> list of [time, size]. The time is when docker received the line, so all containers share
# one clock (the same as the db). Missing times are None, a media without a done line is "unfinished"
def plugin_timings(since):
    timings, queue_sizes = [], []
    for name in run(["docker", "ps", "--format", "{{.Names}}"]).split():
        logs = subprocess.run(["docker", "logs", "--timestamps", "--since", f"{since:.3f}", name],
                              capture_output=True, text=True, encoding="utf-8", errors="replace")
        events = []
        for line in (logs.stdout + logs.stderr).splitlines():  # python logging writes to stderr
            match = TIMING.search(line)
            if match:
                events.append((docker_time(line.split(" ", 1)[0]), *match.groups()))
            elif name == ORCHESTRATOR and (match := QUEUE.search(line)):
                queue_sizes.append([docker_time(line.split(" ", 1)[0]), int(match.group(1))])
        received = {}  # media id -> received times not started yet
        started = {}  # media id -> [received, start] not done yet
        for t, event, media_id, status in sorted(events):
            if event == "received":
                received.setdefault(media_id, []).append(t)
            elif event == "start":
                t_received = received[media_id].pop(0) if received.get(media_id) else None
                started.setdefault(media_id, []).append([t_received, t])
            elif started.get(media_id):
                timings.append([name, media_id, *started[media_id].pop(0), t, status])
        timings += [[name, media_id, t_received, t_start, None, "unfinished"]
                    for media_id, items in started.items() for t_received, t_start in items]
        timings += [[name, media_id, t, None, None, "unfinished"] for media_id, ts in received.items() for t in ts]
    return timings, sorted(queue_sizes)

# face recognition keeps the faces it has seen in KNOWN_FACES and in memory, and the db reset
# clears neither. Without this, later runs match against the faces of earlier runs
def reset_known_faces(timeout=60):
    running = run(["docker", "ps", "--format", "{{.Names}}"]).split()
    faces = next((name for name in FACES if name in running), None)
    if faces is None:
        return  # plugin is not running

    run(["docker", "exec", faces, "sh", "-c", f"rm -f {KNOWN_FACES}/*"])
    run(["docker", "restart", faces])

    # wait until it consumes again. With an exclusive queue it would miss medias before that
    # (docker-compose.yaml). StartedAt is the docker clock, so it matches the log timestamps
    started = run(["docker", "inspect", "-f", "{{.State.StartedAt}}", faces]).strip()
    deadline = time.time() + timeout
    while time.time() < deadline:
        # python logging writes to stderr, so read both
        logs = subprocess.run(["docker", "logs", "--since", started, faces],
                              capture_output=True, text=True, encoding="utf-8", errors="replace")
        if any(ready in logs.stdout + logs.stderr for ready in FACES_READY):
            return
        time.sleep(1)
    raise RuntimeError(f"{faces} did not start consuming within {timeout}s")

# named queues (docker-compose.standalone.yaml) are durable and keep their messages when a run
# is stopped or a plugin restarts, so empty them. The auto-named (amq.gen-) queues are deleted
# with their consumer anyway
def purge_named_queues():
    request = urllib.request.Request(RABBIT + "/api/queues?columns=name", headers={"Authorization": AUTH})
    with urllib.request.urlopen(request, timeout=10) as response:
        names = [q["name"] for q in json.load(response)]
    for name in names:
        if name.startswith("amq."):
            continue
        url = f"{RABBIT}/api/queues/%2F/{urllib.parse.quote(name, safe='')}/contents"
        urllib.request.urlopen(urllib.request.Request(url, method="DELETE", headers={"Authorization": AUTH}),
                               timeout=10)

def reset_and_load_tagsets():
    reset_known_faces()
    purge_named_queues()

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

# dump the whole db to `path` in pg_dump's custom format (compressed). Restore it with
# pg_restore -d <db> db.dump, or list its contents with pg_restore -l db.dump
def dump_db(path):
    with open(path, "wb") as f:
        result = subprocess.run(["docker", "exec", DB, "pg_dump", "-U", "postgres", "-d", "loader-testing", "-Fc"],
                                stdout=f, stderr=subprocess.PIPE)
    if result.returncode != 0:
        raise RuntimeError(f"pg_dump failed: {result.stderr.decode(errors='replace').strip()}")

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

    # rows written per table, counted since the db reset (cumulative)
    db_file = open(run_dir / "db.csv", "w", newline="", encoding="utf-8")
    db_csv = csv.writer(db_file)
    db_csv.writerow(["t_s", "table", "inserted", "updated", "deleted"])

    # read before the start time, it takes a few seconds
    counters_start = cgroup_counters()

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

        tables = db_writes_per_table()
        writes = sum(ins + upd + dele for _, ins, upd, dele in tables)
        for table, ins, upd, dele in tables:
            db_csv.writerow([time_since_start, table, ins, upd, dele])
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
        db_file.flush()

        insert_running = process.poll() is None  # poll() is None while the process is running
        quiet_for = now - last_change
        print(f"time: {time_since_start:.0f}s db writes {writes} quiet for {quiet_for:.0f}s")
        
        if not insert_running and quiet_for >= quiet:
            break

        time.sleep(max(0, poll - (time.time() - now)))

    stats_file.close()
    gpu_file.close()
    queues_file.close()
    db_file.close()

    # cpu time and memory stall time per container during the run (includes the quiet period at the end)
    counters_end = cgroup_counters()
    cpu_total = 0
    with open(run_dir / "cgroup.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["container", "cpu_s", "cpu_user_s", "cpu_system_s", "mem_stall_some_s", "mem_stall_full_s"])
        for name, end_values in sorted(counters_end.items()):
            if name not in counters_start:  # started during the run
                continue
            diff = {key: round((end_values[key] - counters_start[name][key]) / 1e6, 3) for key in end_values}
            writer.writerow([name, diff["usage_usec"], diff["user_usec"], diff["system_usec"],
                             diff["mem_some_usec"], diff["mem_full_usec"]])
            cpu_total += diff["usage_usec"]

    # one row per container and media, times in seconds since the start of the run (db clock).
    # wait = received -> start (time in the plugin's own queue), duration = start -> end
    timings, orchestrator_queue = plugin_timings(start)
    since_start = lambda t: round(t - start, 3) if t is not None else None
    between = lambda a, b: round(b - a, 3) if a is not None and b is not None else None
    busy = {}
    with open(run_dir / "timings.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["container", "media_id", "received_s", "start_s", "end_s", "wait_s", "duration_s", "status"])
        for name, media_id, t_received, t_start, t_end, status in sorted(
                timings, key=lambda r: (r[0], r[2] if r[2] is not None else r[3])):
            duration = between(t_start, t_end)
            writer.writerow([name, media_id, since_start(t_received), since_start(t_start), since_start(t_end),
                             between(t_received, t_start), duration, status])
            busy[name] = round(busy.get(name, 0) + (duration or 0), 2)

    # medias waiting in the orchestrator, one row each time the size changes
    with open(run_dir / "orchestrator_queue.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["t_s", "queued"])
        for t, size in orchestrator_queue:
            writer.writerow([since_start(t), size])

    # get last write from db
    last_write = " UNION ALL ".join(f"SELECT max(pg_xact_commit_timestamp(xmin)) AS ts FROM {t}" for t in TABLES)
    media_count, end = sql(f"SELECT (SELECT count(*) FROM medias), "
                           f"extract(epoch FROM (SELECT max(ts) FROM ({last_write}) w))").split(",")

    result = {
        "media_count": int(media_count),
        "total_s": round(float(end) - start, 2) if end else None,
        "cpu_s": round(cpu_total, 2),  # all containers together, per container in cgroup.csv
        "busy_s": busy,  # per container, sum of the durations in timings.csv
    }
    with open(run_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    # the db state at the end of this run, so the results can be validated later. Taken after
    # all measurements, and the next run resets the db, so every run gets its own dump
    dump_db(run_dir / "db.dump")
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
        writer.writerow(["run", "media_count", "total_s", "cpu_s"])
        for i, result in enumerate(results, start=1):
            writer.writerow([i, result["media_count"], result["total_s"], result["cpu_s"]])
    print(f"Results in {out_dir}")

if __name__ == "__main__":
    main()
