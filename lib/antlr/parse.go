package antlr

import (
	"errors"
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/generated"
	"strings"
)

type ParseError struct {
	err error
}

func newParseError(err error) ParseError {
	return ParseError{err: err}
}

func (p ParseError) Unwrap() error        { return p.err }
func (p ParseError) Error() string        { return fmt.Sprintf("ddl parse error: %s", p.err) }
func (p ParseError) Is(target error) bool { return target == ParseError{} }

func IsParseError(err error) bool {
	return errors.Is(err, ParseError{})
}

func baseUnescape(s string, unescapeChar string) string {
	if strings.Count(s, unescapeChar) == 2 && strings.HasPrefix(s, unescapeChar) && strings.HasSuffix(s, unescapeChar) {
		return s[1 : len(s)-1]
	}

	return s
}

func unescape(s string) string {
	return baseUnescape(s, "`")
}

func Parse(sqlCmd string) ([]Event, error) {
	lexer := generated.NewMySqlLexer(antlr.NewInputStream(sqlCmd))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	// This will go through our custom visit function. If you are trying to print out the AST, split this function into [sqlStatements] and [parser]
	// Then have print [sqlStatements.ToStringTree(nil, parser)]
	return visit(generated.NewMySqlParser(stream).SqlStatements())
}

func visit(tree antlr.Tree) ([]Event, error) {
	switch ctx := tree.(type) {
	case
		*generated.SqlStatementsContext,
		*generated.SqlStatementContext,
		*generated.DdlStatementContext,
		*generated.TransactionStatementContext,
		*generated.BeginWorkContext:
		var events []Event
		for _, child := range ctx.GetChildren() {
			evt, err := visit(child)
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
	case
		*generated.EmptyStatement_Context,
		*generated.CopyCreateTableContext,
		*generated.TruncateTableContext,
		*generated.AdministrationStatementContext,
		*generated.CreateDatabaseContext,
		*antlr.TerminalNodeImpl,
		// Ignoring *generated.DmlStatementContext since it can pick up
		// INSERT INTO mysql.rds_heartbeat2(id, value)
		*generated.DmlStatementContext,
		*generated.CommitWorkContext:
		return nil, nil
	default:
		return nil, newParseError(fmt.Errorf("unsupported context type: %T", ctx))
	}
}
