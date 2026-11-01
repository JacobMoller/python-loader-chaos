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

`--folder` is the path inside the containers. Before each run the database is reset and the tagsets are created. Other options: `--poll` (seconds between samples, default 1; in practice about 2 s, because `docker stats --no-stream` takes that long per call) and `--quiet` (default 60; a plugin that works longer than this without writing would end a run early).

## Output

`bench_results/<label>_<date>_<time>/`:

- `runs.csv`: media count and `total_s` per run
- `run_XX/summary.json`: the same for one run
- `run_XX/stats.csv`: CPU % (100 % = one core) and memory per container, about every 2 s
- `run_XX/gpu.csv`: GPU utilization and GPU memory, about every 2 s (empty without an NVIDIA GPU)
- `run_XX/queues.csv`: per RabbitMQ queue, messages `ready` (waiting) and `unacked` (delivered to a plugin, not yet acknowledged) and the number of consumers, about every 2 s. RabbitMQ refreshes these numbers every ~5 s. In the current version the queues have random names (`amq.gen-…`) and stay at 0 because of `auto_ack`; with named queues per plugin they show each plugin's backlog
