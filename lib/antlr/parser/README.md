# Antlr Parser

To generate these files, we'll need to do the following:

* Install antlr `brew install antlr`
* Git clone `https://github.com/antlr/grammars-v4/tree/master`

```bash
cd grammars-v4
cd grammars-v4/sql/mysql/Positive-Technologies

antlr -Dlanguage=Go -o output/ MySqlLexer.g4 MySqlParser.g4 -o listener
cp output/*.go /path/to/this/repo/lib/antlr/parser
```
