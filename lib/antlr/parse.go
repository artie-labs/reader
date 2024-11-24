package antlr

import (
	"fmt"
	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/artie-labs/reader/lib/antlr/parser"
)

type sqlListener struct {
	*parser.BaseMySqlParserListener
}

func (l *sqlListener) EnterEveryRule(ctx antlr.ParserRuleContext) {
	fmt.Println("Entering rule:", ctx.GetText())
}

func main() {
	// Sample SQL input
	input := "SELECT * FROM users WHERE id = 1"

	// Create an input stream from the SQL input
	is := antlr.NewInputStream(input)

	// Create a lexer instance
	lexer := parser.NewMySqlLexer(is)

	// Create a token stream from the lexer
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create a parser instance
	p := parser.NewMySqlParser(stream)

	// Create a listener
	listener := &sqlListener{}

	// Walk the parse tree
	antlr.ParseTreeWalkerDefault.Walk(listener, p.Root())
}
