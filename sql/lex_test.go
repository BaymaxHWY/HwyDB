package sql

import (
	"fmt"
	"log"
	"testing"
	"unicode/utf8"
)

func Test_lexer(t *testing.T) {
	sql1 := "insert age 29;"
	sql2 := "find age"
	sql3 := "update name '吴';"
	sql4 := "delete age;"
	fmt.Println("parse sql :", sql1)
	lexer := NewLex(sql1)
	printTokens(lexer.tokens)
	fmt.Println("parse sql :", sql2)
	lexer = NewLex(sql2)
	printTokens(lexer.tokens)
	fmt.Println("parse sql :", sql3)
	lexer = NewLex(sql3)
	printTokens(lexer.tokens)
	fmt.Println("parse sql :", sql4)
	lexer = NewLex(sql4)
	printTokens(lexer.tokens)
}

func printTokens(tokens []*token) {
	for _, token := range tokens {
		fmt.Printf("%s", token)
	}
	fmt.Println()
}

func Test_isVariable(t *testing.T) {
	str := "hello, woRld ! 你好，世界 _hhhh"
	pos := 0
	for pos < len(str) {
		r, w := utf8.DecodeRuneInString(str[pos:])
		pos += w
		log.Printf("%q is variable : %v\n", r, isVariable(r))
	}
}

func Test_isSymbol(t *testing.T) {
	str := "hello, woRld ! 你好，世界 _hhhh, 1 >= 2, 1 + 2 - 4 * 6 / 10 % 200"
	pos := 0
	for pos < len(str) {
		r, w := utf8.DecodeRuneInString(str[pos:])
		pos += w
		log.Printf("%q is variable : %v\n", r, isSymbol(r))
	}
}
