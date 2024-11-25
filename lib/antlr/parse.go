package main

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/artie-labs/reader/lib/antlr/parser"
)

type Event interface {
	GetTable() string
	GetColumns() []Column
}

type Column struct {
	Name     string
	DataType string
}

type CreateTableEvent struct {
	TableName string
	Columns   []Column
}

type DropColumnsEvent struct {
	TableName string
	Columns   []Column
}

type AddColumnsEvent struct {
	TableName string
	Columns   []Column
}

type ChangedColumnsEvent struct {
	TableName string
	Columns   []Column
}

func Parse(input string) {
	is := antlr.NewInputStream(input)
	lexer := parser.NewMySqlLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)

	// Create a parser instance
	sqlParser := parser.NewMySqlParser(stream)
	tree := sqlParser.Root() // Replace `Root` with your grammar's root rule
	// Print the parse tree (or process it as needed)
	fmt.Println(tree.ToStringTree(nil, sqlParser))
}
