# arch_graph.py

Static architecture recovery for Python code. Parses every `.py` file with `ast`, finds imports that point to other files in the repo, and renders them as an interactive dependency graph (HTML).

Only internal imports are shown; standard library and third-party imports are skipped.

## Usage

Requires Python 3, no extra packages. Run from the repo root:

```bash
python3 tools/arch_graph.py --depth 2
open arch_graph.html
```

## Options

| Option | Description | Default |
|---|---|---|
| `--depth N` | Path levels to keep. Deeper files are merged into their folder at level N | `2` |
| `--ignore X` | Skip a folder by name (`tests`) or repo-relative path (`client/tests`). Repeatable | none |
| `--search-path X` | Extra folder to resolve imports from, like `PYTHONPATH`. Repeatable | none |
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

## Example for this repo

The plugins in `go-server/plugins` import `rabbitMQ_helpers`, which Docker copies into each plugin folder at build time. Use `--search-path` to resolve it:

```bash
python3 tools/arch_graph.py --depth 3 --ignore client/tests --ignore tools \
  --ignore go-server/dataloader --search-path go-server/rabbitMQ --hide-isolated \
  -o graphs/depth3.html
```

## Limitations

- Only static `import` statements; dynamic imports (`importlib`, `__import__`) are not detected
- Imports are resolved from the importing file's folder upward to the repo root, plus any `--search-path`
- The HTML loads `vis-network` from a CDN, so it needs an internet connection to open
