package sql

import (
	"HwyDB/index"
	"fmt"
	"testing"
)

func Test_parseAST(t *testing.T) {
	cases := []struct{
		name string
		sql string
	}{
		{"insert1", "insert age1 1"},
		{"insert2", "insert age2 2"},
		{"insert3", "insert age3 3"},
		{"insert4", "insert age4 4"},
		{"insert5", "insert name1 '何惟禹'"},
		{"insert6", "insert name2 '吴恕'"},
		{"insert7", "insert name3 '傻逼'"},
		{"insert8", "insert name4 'dd'"},
		{"insert9", "insert name5 '你是谁'"},
		{"insert10", "insert sex1 '男'"},
		{"insert11", "insert sex2 '女'"},
		{"find1", "find age3"},
		{"find2", "find name4"},
		{"find3", "find sex1"},
		{"update1", "update sex1 '女'"},
		{"find4", "find sex1"},
		{"delete1", "delete name4"},
		{"delete2", "delete name3"},
		{"delete3", "delete name2"},
		{"find5", "find name2"},
		{"insert6", "insert name2 '吴恕'"},
		{"find5", "find name2"},
	}
	bt := index.New(3)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fmt.Println("exec:", c.sql)
			lex := NewLex(c.sql)
			//fmt.Println(lex.tokens)
			tokenReader := NewTokenReader(lex.tokens)
			root, err := tokenReader.buildAST()
			if err != nil {
				t.Fatal(err)
			}
			ret := parseAST(root, bt)
			fmt.Println("ret:", ret)
		})
	}
}

func Test_buildAST(t *testing.T) {
	cases := []struct{
		name string
		sql string
	}{
		{"insert", "insert age 29"},
		{"insert", "insert name '何惟禹'"},
		{"find", "find age"},
		{"find", "find name"},
		{"update", "update age 30"},
		{"delete", "delete name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			lex := NewLex(c.sql)
			tokenReader := NewTokenReader(lex.tokens)
			root, err := tokenReader.buildAST()
			if err != nil {
				t.Fatal(err)
			}
			printAST(root, 0)
		})
	}
}

func printAST(root *SynatxTreeNode, tabs int) {
	if root == nil {
		return
	}
	tab := ""
	for i := 0; i < tabs; i++ {
		tab += "\t"
	}
	if root.Value != nil {
		fmt.Println(tab+root.Name, ":", root.Value)
	} else {
		fmt.Println(tab + root.Name)
	}
	for _, c := range root.Child {
		printAST(c, tabs + 1)
	}
}