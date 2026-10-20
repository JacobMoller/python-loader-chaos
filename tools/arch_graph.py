"""
Static architecture recovery: builds an interactive dependency graph of the
internal Python or Go imports in a repository.

Usage:
    python tools/arch_graph.py --depth 2
    python tools/arch_graph.py --root . --depth 3 --ignore protos --ignore client/tests -o graph.html
    python tools/arch_graph.py --lang go --depth 3 -o go_graph.html
"""

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from collections import Counter

GO_IMPORTS_HELPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "go_imports", "main.go")

folder_color = "#B0B0B0"  # color for folders in the graph
file_color = "#4C8EDA"

# ---------------------------------------------------------------- file discovery

def find_source_files(root, extra_ignores, suffixes=(".py",)):
    """Return repo-relative paths of files ending in `suffixes` that are not ignored."""
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        rel_dir = "" if rel_dir == "." else rel_dir

        # Prune ignored folders, .git, and virtualenvs (folders with a pyvenv.cfg)
        kept = []
        for d in dirnames:
            rel = os.path.join(rel_dir, d)
            if d == ".git" or d in extra_ignores or rel in extra_ignores:
                continue
            if os.path.exists(os.path.join(dirpath, d, "pyvenv.cfg")):
                continue
            kept.append(d)
        dirnames[:] = kept

        for f in filenames:
            if f.endswith(suffixes):
                files.append(os.path.join(rel_dir, f))

    return filter_gitignored(root, files)


def filter_gitignored(root, files):
    """Drop files matched by .gitignore (also files that are tracked but match a pattern)."""
    try:
        result = subprocess.run(
            ["git", "check-ignore", "--no-index", "--stdin"],
            cwd=root, input="\n".join(files), capture_output=True, text=True,
        )
    except FileNotFoundError:
        return files
    ignored = set(result.stdout.splitlines())
    return [f for f in files if f not in ignored]


# ---------------------------------------------------------------- python imports

def python_imports(path, rel_path):
    """Yield module names imported by a Python file (relative imports resolved to dotted names)."""
    with open(path, "rb") as fh:
        try:
            tree = ast.parse(fh.read(), filename=rel_path)
        except (SyntaxError, ValueError) as e:
            print(f"  skipped (parse error): {rel_path}: {e}")
            return

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative import: anchor at the file's own package
                pkg_parts = os.path.dirname(rel_path).split(os.sep)
                pkg_parts = pkg_parts[: len(pkg_parts) - (node.level - 1)]
                base = ".".join(p for p in pkg_parts if p)
                module = f"{base}.{node.module}" if node.module else base
            else:
                module = node.module
            # "from pkg import mod" may refer to a submodule, so try both
            for alias in node.names:
                yield f"{module}.{alias.name}"
            yield module


def resolve_python(module, rel_path, py_files, dirs, search_paths=()):
    """Map a dotted module name to an internal file or folder, or None if external."""
    parts = module.split(".")
    # Search from the importing file's folder upward to the repo root,
    # then any extra search paths (like PYTHONPATH)
    base = os.path.dirname(rel_path)
    bases = []
    while True:
        bases.append(base)
        if not base:
            break
        base = os.path.dirname(base)
    bases.extend(search_paths)

    for base in bases:
        candidate = os.path.join(base, *parts)
        if candidate + ".py" in py_files:
            return candidate + ".py"
        if os.path.join(candidate, "__init__.py") in py_files:
            return os.path.join(candidate, "__init__.py")
        # A folder that contains the importing file is almost always a same-named
        # third-party package (e.g. plugins/face_recognition importing face_recognition)
        if candidate in dirs and not rel_path.startswith(candidate + os.sep):
            return candidate
    return None


# ---------------------------------------------------------------- go imports

def go_imports(root, go_files):
    """Return {file: [import paths]}, parsed with Go's own go/parser via tools/go_imports."""
    try:
        result = subprocess.run(
            ["go", "run", GO_IMPORTS_HELPER],
            cwd=root, input="\n".join(go_files), capture_output=True, text=True,
        )
    except FileNotFoundError:
        sys.exit("Go is not installed (needed for --lang go)")
    if result.stderr:
        print(result.stderr, end="")
    if result.returncode != 0:
        sys.exit("go_imports helper failed")
    return {os.path.normpath(f): imps for f, imps in json.loads(result.stdout).items()}


def read_go_modules(root, mod_files):
    """Return {module_path: module_dir} for every go.mod."""
    modules = {}
    for f in mod_files:
        with open(os.path.join(root, f), encoding="utf-8") as fh:
            m = re.search(r"^module\s+(\S+)", fh.read(), re.M)
        if m:
            modules[m.group(1)] = os.path.dirname(f)
    return modules


def resolve_go(import_path, modules, dirs):
    """Map a Go import path to an internal package folder, or None if external."""
    for mod_path in sorted(modules, key=len, reverse=True):
        if import_path == mod_path or import_path.startswith(mod_path + "/"):
            rest = import_path[len(mod_path):].lstrip("/")
            target = os.path.normpath(os.path.join(modules[mod_path], rest))
            return target if target in dirs else None
    return None


# ---------------------------------------------------------------- graph

def collapse(path, depth):
    """Cut a path down to its first `depth` components (files deeper become their folder)."""
    parts = path.split(os.sep)
    return os.sep.join(parts[:depth])


def count_loc(path):
    """Lines of code: non-blank lines."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        return sum(1 for line in fh if line.strip())


def build_graph(root, depth, extra_ignores, lang="python", search_paths=(), hide_isolated=False):
    if lang == "go":
        files = find_source_files(root, extra_ignores, (".go", "go.mod"))
        sources = [f for f in files if f.endswith(".go")]
        modules = read_go_modules(root, [f for f in files if f.endswith("go.mod")])
    else:
        sources = find_source_files(root, extra_ignores)
    dirs = set()
    for f in sources:
        d = os.path.dirname(f)
        while d:
            dirs.add(d)
            d = os.path.dirname(d)

    # Raw edges: (source file, target file/folder), counted once per file
    raw_edges = set()
    if lang == "go":
        print(f"Analyzing {len(sources)} Go files in {len(modules)} module(s)")
        for f, imports in go_imports(root, sources).items():
            for t in {resolve_go(m, modules, dirs) for m in imports}:
                if t:
                    raw_edges.add((f, t))
    else:
        print(f"Analyzing {len(sources)} Python files")
        py_files = set(sources)
        for f in sources:
            full = os.path.join(root, f)
            targets = {resolve_python(m, f, py_files, dirs, search_paths)
                       for m in python_imports(full, f) if m}
            for t in targets:
                if t and t != f:
                    raw_edges.add((f, t))

    # Go imports point at packages (folders), so Go files are shown as their package
    def unit(f):
        return (os.path.dirname(f) or ".") if lang == "go" else f

    # Collapse to the requested depth and sum edge weights
    nodes = Counter(collapse(unit(f), depth) for f in sources)
    loc = Counter()
    for f in sources:
        loc[collapse(unit(f), depth)] += count_loc(os.path.join(root, f))
    edges = Counter()
    for src, dst in raw_edges:
        a, b = collapse(unit(src), depth), collapse(dst, depth)
        if a != b:
            edges[(a, b)] += 1
            nodes.setdefault(b, 0)

    if hide_isolated:
        connected = {n for edge in edges for n in edge}
        nodes = Counter({n: c for n, c in nodes.items() if n in connected})
    return nodes, edges, loc, sources


# ---------------------------------------------------------------- html output

HTML_TEMPLATE = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Architecture graph</title>
<script src="https://unpkg.com/vis-network@9.1.9/standalone/umd/vis-network.min.js"></script>
<style>
  html, body { margin: 0; height: 100%; font-family: sans-serif; background: #fff; }
  #graph { position: absolute; inset: 0; }
  #legend { position: absolute; top: 10px; left: 10px; background: #fffe; padding: 8px 12px;
            border: 1px solid #ddd; border-radius: 6px; font-size: 13px; z-index: 1; }
  .dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
</style>
</head>
<body>
<div id="legend">
  <b>__TITLE__</b><br>
  __LEGEND_ITEMS__
  Node size = lines of code<br>
  Edge width and label = number of importing files
</div>
<div id="graph"></div>
<script>
const data = __DATA__;
new vis.Network(document.getElementById("graph"),
  { nodes: new vis.DataSet(data.nodes), edges: new vis.DataSet(data.edges) },
  {
    nodes: { shape: "dot", font: { size: 14 }, scaling: { min: 8, max: 60 } },
    edges: { arrows: "to", color: { color: "#999", highlight: "#e66" },
             scaling: { min: 1, max: 12 },
             font: { size: 11, align: "middle" }, smooth: { type: "dynamic" } },
    physics: { solver: "forceAtlas2Based", stabilization: { iterations: 300 } },
    interaction: { hover: true, navigationButtons: true },
  });
</script>
</body>
</html>
"""


def legend_item(color, text):
    return f'<span class="dot" style="background:{color}"></span>{text}<br>'


def write_html(nodes, edges, loc, sources, depth, out_path, lang="python"):
    source_set = set(sources)
    vis_nodes = []
    for node, file_count in sorted(nodes.items()):
        if node in source_set:
            color = file_color
            tip = f"{node} ({loc[node]} LOC)"
        else:
            color = folder_color
            tip = f"{node}/ ({file_count} source files, {loc[node]} LOC)"
        vis_nodes.append({"id": node, "label": node, "title": tip, "color": color,
                          "value": loc[node]})

    vis_edges = [{"from": a, "to": b, "label": str(w), "value": w,
                  "title": f"{a} → {b}: {w} importing file(s)"}
                 for (a, b), w in edges.items()]

    if lang == "go":
        legend = legend_item(folder_color, "Go package (folder)")
    else:
        legend = legend_item(file_color, "Python file") + legend_item(folder_color, "Folder")

    html = (HTML_TEMPLATE
            .replace("__TITLE__", f"{'Go' if lang == 'go' else 'Python'} dependencies (depth {depth})")
            .replace("__LEGEND_ITEMS__", legend)
            .replace("__DATA__", json.dumps({"nodes": vis_nodes, "edges": vis_edges})))
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(html)


# ---------------------------------------------------------------- main

def main():
    parser = argparse.ArgumentParser(description="Build an internal dependency graph for Python or Go code.")
    parser.add_argument("--lang", choices=["python", "go"], default="python", help="language to analyze")
    parser.add_argument("--root", default=".", help="repository root (default: current directory)")
    parser.add_argument("--depth", type=int, default=2,
                        help="number of path levels to keep; deeper files are merged into their folder")
    parser.add_argument("--ignore", action="append", default=[],
                        help="folder name or repo-relative folder path to skip (repeatable)")
    parser.add_argument("--search-path", action="append", default=[],
                        help="Python only: extra repo-relative folder to resolve imports from, like PYTHONPATH (repeatable)")
    parser.add_argument("--hide-isolated", action="store_true", help="hide nodes without any edges")
    parser.add_argument("-o", "--output", default="arch_graph.html", help="output HTML file")
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    ignores = {os.path.normpath(p) for p in args.ignore}
    search_paths = [os.path.normpath(p) for p in args.search_path]
    nodes, edges, loc, sources = build_graph(root, args.depth, ignores, args.lang,
                                             search_paths, args.hide_isolated)
    write_html(nodes, edges, loc, sources, args.depth, args.output, args.lang)
    print(f"Wrote {len(nodes)} nodes and {len(edges)} edges to {args.output}")


if __name__ == "__main__":
    main()


