package antlr

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
)

func parseDefaultValue(node generated.IDefaultValueContext) string {
	for _, child := range node.GetChildren() {
		switch castedChild := child.(type) {
		case
			*generated.CurrentTimestampContext,
			*antlr.TerminalNodeImpl:
			continue
		case *generated.ConstantContext:
			return castedChild.GetText()
		default:
			slog.Warn("Skipping default value that is not a constant", slog.String("type", fmt.Sprintf("%T", child)))
		}
	}

	return ""
}

// getTextFromSingleNodeBranch - Will visit the entire branch and return the text when it reaches the terminal node.
// This will error out if the tree has more than one child.
func getTextFromSingleNodeBranch(tree antlr.Tree) (string, error) {
	if end, ok := tree.(*antlr.TerminalNodeImpl); ok {
		return end.String(), nil
	}

	if tree.GetChildCount() != 1 {
		return "", fmt.Errorf("unexpected number of children: %d", tree.GetChildCount())
	}

	return getTextFromSingleNodeBranch(tree.GetChild(0))
}

func getTableNameFromNode(ctx generated.ITableNameContext) (string, error) {
	children := ctx.GetChildren()
	if len(children) != 1 {
		return "", fmt.Errorf("unexpected number of children: %d", len(children))
	}

	var parts []string
	for _, node := range children[0].GetChildren() {
		part, err := getTextFromSingleNodeBranch(node)
		if err != nil {
			return "", err
		}

		parts = append(parts, part)
	}

	switch len(parts) {
	case 1:
		return strings.TrimPrefix(parts[0], "."), nil
	case 2:
		return strings.TrimPrefix(parts[1], "."), nil
	case 3:
		return strings.TrimPrefix(parts[2], "."), nil
	default:
		return "", fmt.Errorf("unexpected number of parts: %d, value: [%s]", len(parts), strings.Join(parts, ", "))
	}
}
