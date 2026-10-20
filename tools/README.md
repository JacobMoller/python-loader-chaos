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