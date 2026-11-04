# arch_graph.py

Static architecture recovery for Python or Go code. Parses source files with an AST (Python's `ast`, or Go's `go/parser` via the helper in `go_imports/`), finds imports that point to other files in the repo, and renders them as an interactive dependency graph (HTML).

Only internal imports are shown; standard library and third-party imports are skipped.

## Usage

Requires Python 3, no extra packages. Go analysis also requires Go. Run from the repo root:

```bash
python3 tools/arch_graph.py --depth 2
open arch_graph.html

python3 tools/arch_graph.py --lang go --depth 3 -o go_graph.html
open go_graph.html
```

## Options

| Option | Description | Default |
|---|---|---|
| `--lang L` | `python` or `go` | `python` |
| `--depth N` | Path levels to keep. Deeper files are merged into their folder at level N | `2` |
| `--ignore X` | Skip a folder by name (`tests`) or repo-relative path (`client/tests`). Repeatable | none |
| `--search-path X` | Python only. Extra folder to resolve imports from, like `PYTHONPATH`. Repeatable | none |
| `--hide-isolated` | Hide nodes without any edges | off |
| `-o FILE` | Output HTML file | `arch_graph.html` |
| `--root DIR` | Repository to analyze | `.` |

## What is skipped

- Files matched by `.gitignore` (also if they are committed)
- Virtualenvs (folders containing `pyvenv.cfg`) and `.git`
- Folders passed with `--ignore`

## Reading the graph

- **Blue node:** Python file. **Grey node:** folder (collapsed by `--depth`)
- **Node size:** lines of code (non-blank lines; summed for folders)
- **Edge A → B:** A imports B. Width and label show the number of files behind that import

### Go

Go imports point at packages (folders), and files in the same package use each other without imports. So in Go mode every node is a package folder: a Go file is always counted under its package, and `--depth` then collapses package folders further. Imports are matched to internal packages using the `module` path in each `go.mod`.

## Example for this repo

The plugins in `go-server/plugins` import `rabbitMQ_helpers`, which Docker copies into each plugin folder at build time. Use `--search-path` to resolve it:

```bash
python3 tools/arch_graph.py --depth 3 --ignore client/tests --ignore tools \
  --ignore go-server/dataloader --search-path go-server/rabbitMQ --hide-isolated \
  -o graphs/tools/depth3.html
```

Go dependencies for the same repo:

```bash
python3 tools/arch_graph.py --lang go --depth 3 --ignore tools -o graphs/go_depth3.html
```

## Limitations

- Only static `import` statements; dynamic imports (`importlib`, `__import__`) are not detected
- Imports are resolved from the importing file's folder upward to the repo root, plus any `--search-path`
- The HTML loads `vis-network` from a CDN, so it needs an internet connection to open



# Class diagrams
Using pyreverse from pylint to generate class diagrams for the `client`.
Using goplantuml to generate plantuml for the `go-server`.

## Python
Prerequisites:
```bash
python -m pip install pylint    
```

```bash
pyreverse -o png -p client client/filemgmt client/grpc_client.py client/sysmon.py client/cli.py          
```

## GO
Prerequisites:
```bash
go install github.com/jfeliu007/goplantuml/cmd/goplantuml@latest   
brew install plantuml
```
Ignoring dataloader and media_downloader/gen as this is auto generated code.
```bash
~/go/bin/goplantuml -recursive -ignore dataloader,media_downloader/gen . > go.puml
plantuml go.puml 
```



# What to run to get same results in graph folder:

```bash
python3 tools/arch_graph.py --depth 3 --ignore client/tests --ignore tools --ignore server --search-path go-server/rabbitMQ -o tools/graphs/depth3.html
```

```bash
python3 tools/arch_graph.py --depth 2 --ignore client/tests --ignore tools --ignore server --search-path go-server/rabbitMQ -o tools/graphs/depth2.html
```

```bash
python3 tools/arch_graph.py --lang go --depth 3 --ignore tools -o tools/graphs/go_depth3.html
```

```bash
~/go/bin/goplantuml -recursive -ignore dataloader,media_downloader/gen . > go.puml
plantuml go.puml 
```

```bash
pyreverse -o png -p client client/filemgmt client/grpc_client.py client/sysmon.py client/cli.py          
```
# e2e_benchmark.py
Script used for experiment E3.
Measures the end-to-end time from queueing a folder of medias (`cli.py add medias`) until the pipeline has processed all of them. It also records CPU/memory per container and GPU while it runs (if machine has GPU). It observes the running stack.

- **End time:** `docker-compose.yaml` needs to start Postgres with `track_commit_timestamp=on`, so every row remembers when it was written. After a run, the latest commit time in `medias`, `taggings`, `tags` and `nodes` is the end.
- **When to stop waiting:** a run is over when the insert has finished and the database has had no writes for `--quiet` seconds. The waiting time is not part of the result.

## Usage
After changing `docker-compose.yaml`, recreate the database container once with `docker compose up -d db`. Put the medias in a folder under `medias/`, then from the repo root:

```bash
python tools/e2e_benchmark.py --folder /app/medias/bench_1000 --runs 5 --label baseline
```

`--folder` is the path inside the containers. Before each run the database is reset and the tagsets are created. Face recognition also remembers every face it has seen (in the `known_faces` volume and in memory), so its folder is emptied and the plugin restarted, and the run waits until it listens again. Other options: `--poll` (seconds between samples, default 1; in practice about 2 s, because `docker stats --no-stream` takes that long per call) and `--quiet` (default 60; a plugin that works longer than this without writing would end a run early).

## Output

`bench_results/<label>_<date>_<time>/`:

- `runs.csv`: media count, `total_s` and `cpu_s` (CPU time of all containers together) per run
- `run_XX/summary.json`: the same for one run
- `run_XX/cgroup.csv`: per container the exact CPU time (`cpu_s` = `cpu_user_s` + `cpu_system_s`, in core-seconds) and how long its processes were stalled waiting for memory (`mem_stall_some_s`: at least one process, `mem_stall_full_s`: all of them). Read from the container's cgroup (`cpu.stat` and `memory.pressure`) right before the start and after the end of the run, so it includes the `--quiet` period, when the containers are almost idle
- `run_XX/stats.csv`: CPU % (100 % = one core) and memory per container, about every 2 s
- `run_XX/gpu.csv`: GPU utilization and GPU memory, about every 2 s (empty without an NVIDIA GPU)
- `run_XX/timings.csv`: per container and media, when the plugin received, started and finished it (`received_s`, `start_s`, `end_s`, seconds since the start of the run), `wait_s` (received → start, time in the plugin's own queue), `duration_s` (start → end) and `status` (`ok`, `error`, `skipped`, `timeout` for the orchestrator, `unfinished` if there is no done line). The plugins log `TIMING received ID=<media>`, `TIMING start ID=<media>` and `TIMING done ID=<media> status=<status>`. Only the orchestrator and the classifier have their own queue (the consumer thread receives, a worker starts); the other plugins process a message in the callback, so for them `wait_s` is ~0. All consumers use `auto_ack`, so RabbitMQ pushes every message to the plugin at once, and a message that waits inside the RabbitMQ client library before the callback (e.g. color-detector and exif-extractor at the start of a run) counts as not received yet; after the run the script reads these with `docker logs --timestamps`, so the times come from docker and all containers share one clock with the db. For the orchestrator a row is the whole round of one media (download, dispatch, waiting for the ACKs, release). `summary.json` has the sum of the durations per container as `busy_s`
- `run_XX/orchestrator_queue.csv`: how many medias are waiting in the orchestrator (`queued`), one row each time it changes. RabbitMQ hands all `media.*` messages to the orchestrator at once, so this backlog is not visible in `queues.csv`. Read from the `QUEUE size=<n>` lines the orchestrator logs
- `run_XX/db.csv`: per table, the rows `inserted`, `updated` and `deleted` since the database reset (cumulative, so the difference between two samples is what was written in between), about every 2 s. Postgres updates these counters with a delay of up to ~0.5 s
- `run_XX/queues.csv`: per RabbitMQ queue, messages `ready` (waiting) and `unacked` (delivered to a plugin, not yet acknowledged) and the number of consumers, about every 2 s. RabbitMQ refreshes these numbers every ~5 s. In the current version the queues have random names (`amq.gen-…`) and stay at 0 because of `auto_ack`; with named queues per plugin they show each plugin's backlog
