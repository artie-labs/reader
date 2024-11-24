package antlr

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/parser"
)

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
	sqlParser := parser.NewMySqlParser(stream)

	tree := sqlParser.Root() // Replace `Root` with your grammar's root rule

	// Print the parse tree (or process it as needed)
	fmt.Println(tree.ToStringTree(nil, sqlParser))
}
