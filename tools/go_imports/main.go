// Prints the imports of Go source files as JSON ({"path": ["import", ...]}),
// parsed with go/parser. Reads one file path per line from stdin.
// Used by arch_graph.py --lang go.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"strconv"
)

func main() {
	result := map[string][]string{}
	fset := token.NewFileSet()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		path := scanner.Text()
		if path == "" {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  skipped (parse error): %v\n", err)
			continue
		}
		imports := []string{}
		for _, imp := range file.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			imports = append(imports, p)
		}
		result[path] = imports
	}

	json.NewEncoder(os.Stdout).Encode(result)
}
