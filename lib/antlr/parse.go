package antlr

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
	"strings"
)

func unescape(s string) string {
	if strings.Count(s, "`") == 2 && strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") {
		// Remove backticks if they are present
		return s[1 : len(s)-1]
	}

	return s
}

func Parse(sqlCmd string) ([]Event, error) {
	lexer := generated.NewMySqlLexer(antlr.NewInputStream(sqlCmd))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	sqlParser := generated.NewMySqlParser(stream)
	return mySQLVisitor{}.Visit(sqlParser.SqlStatements())
}

type mySQLVisitor struct{}

func (m mySQLVisitor) Visit(tree antlr.Tree) ([]Event, error) {
	switch ctx := tree.(type) {
	case *generated.SqlStatementsContext, *generated.SqlStatementContext, *generated.DdlStatementContext:
		var events []Event
		for _, child := range ctx.GetChildren() {
			evt, err := m.Visit(child)
			if err != nil {
				return nil, err
			}

			if evt != nil {
				events = append(events, evt...)
			}
		}

		return events, nil
	case *generated.ColumnCreateTableContext:
		evt, err := processCreateTable(ctx)
		if err != nil {
			return nil, err
		}

		return []Event{evt}, nil
	case *generated.AlterTableContext:
		return processAlterTable(ctx)
	case *generated.DropTableContext:
		return processDropTable(ctx)
	case *generated.EmptyStatement_Context,
		*generated.CopyCreateTableContext:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported context type: %T", ctx)
	}
}
