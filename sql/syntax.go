package sql

import (
	"HwyDB/index"
	"errors"
	"fmt"
)

// 1.生成语法树
// 2.根据生成的语法树执行对应的方法
// select into age Value(29)
/*
      select
	/		\
  key		Value
{'age',    {29,int}
'string'}
*/

type SynatxTreeNode struct {
	Name      string            // insert、update、key、value等
	Child     []*SynatxTreeNode // 子节点
	Value     interface{}       // 实际值
	ValueType int               // 值的类型：int、float、string
}

type TokenReader struct {
	data []*token // 存储lex生成的tokens
	pos int // 记录读取到的tokens的位置
}

func (t *TokenReader) read() *token {
	t.pos++
	if t.pos > len(t.data) {
		return nil
	}
	return t.data[t.pos - 1]
}

func (t *TokenReader) buildAST() (*SynatxTreeNode, error) {
	if t.data[0].typ != KeyWord {
		return nil, errors.New("需要关键字开头")
	}
	var root *SynatxTreeNode
	var err error
	switch t.data[0].lit {
	case "find":
		root, err = findParser(t)
	case "insert":
		root, err = insertParser(t)
	case "update":
		root, err = updateParser(t)
	case "delete":
		root, err = deleteParser(t)
	default:
		return nil, errors.New("You have a syntax error near: " + t.data[0].lit)
	}
	if err != nil {
		return nil, err
	}
	return root, nil
}

func NewTokenReader(data []*token) *TokenReader {
	return &TokenReader{
		data: data,
		pos:  0,
	}
}
// 解析语法树并执行相应函数
func parseAST(root *SynatxTreeNode, bt index.BT) (interface{}) {
	action := root.Name
	var ret interface{}
	switch action {
	case "insert":
		key, err := getChildForName(root, "key")
		if err != nil {
			fmt.Println("insert error")
			return nil
		}
		value, err := getChildForName(root, "value")
		if err != nil {
			fmt.Println("insert error")
			return nil
		}
		//fmt.Printf("exec insert %s : %v\n", key, value)
		bt.Insert(key, value)
	case "find":
		key, err := getChildForName(root, "key")
		if err != nil {
			fmt.Println("find error")
			return nil
		}
		//fmt.Printf("exec find %s\n", key)
		ret = bt.Find(key)
		if ret == nil {
			fmt.Println("the key:", key, "is not exist")
		}
	case "update":
		key, err := getChildForName(root, "key")
		if err != nil {
			fmt.Println("update error")
			return nil
		}
		value, err := getChildForName(root, "value")
		if err != nil {
			fmt.Println("update error")
			return nil
		}
		//fmt.Printf("exec update %s = %v\n", key, value)
		bt.Update(key, value)
	case "delete":
		key, err := getChildForName(root, "key")
		if err != nil {
			fmt.Println("delet error")
			return nil
		}
		//fmt.Printf("exec delet %s\n", key)
		bt.Delete(key)
	}
	return ret
}
// 根据child的name获取child的value值
func getChildForName(root *SynatxTreeNode, name string) (interface{}, error) {
	if root == nil {
		return nil, errors.New("no get the " + name)
	}
	if root.Name == name {
		return root.Value, nil
	}
	var need interface{}
	var err error
	for _, c := range root.Child {
		 need, err = getChildForName(c, name)
		 if(err == nil) {
		 	return need, err
		 }
	}
	return nil, errors.New("no get the " + name)
}
// -----------关键字解析函数---------------

func insertParser(tr *TokenReader) (*SynatxTreeNode, error) {
	t := tr.read()
	if t.typ != KeyWord || t.lit != "insert" {
		return nil, errors.New("You have a syntax error near: " + t.lit)
	}
	tKey := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tKey.lit)
	}
	tValue := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tValue.lit)
	}
	return &SynatxTreeNode{
		Name:      "insert",
		Child:     []*SynatxTreeNode{&SynatxTreeNode{
			Name:      "key",
			Value:     tKey.lit,
			ValueType: 0,
		},&SynatxTreeNode{
			Name:      "value",
			Value:     tValue.lit,
			ValueType: 1,
		}},
	}, nil
}

func findParser(tr *TokenReader) (*SynatxTreeNode, error) {
	t := tr.read()
	if t.typ != KeyWord || t.lit != "find" {
		return nil, errors.New("You have a syntax error near: " + t.lit)
	}
	tKey := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tKey.lit)
	}
	return &SynatxTreeNode{
		Name:      "find",
		Child:     []*SynatxTreeNode{&SynatxTreeNode{
			Name:      "key",
			Value:     tKey.lit,
			ValueType: 0,
		}},
	}, nil
}

func updateParser(tr *TokenReader) (*SynatxTreeNode, error) {
	t := tr.read()
	if t.typ != KeyWord || t.lit != "update" {
		return nil, errors.New("You have a syntax error near: " + t.lit)
	}
	tKey := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tKey.lit)
	}
	tValue := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tValue.lit)
	}
	return &SynatxTreeNode{
		Name:      "update",
		Child:     []*SynatxTreeNode{&SynatxTreeNode{
			Name:      "key",
			Value:     tKey.lit,
			ValueType: 0,
		},&SynatxTreeNode{
			Name:      "value",
			Value:     tValue.lit,
			ValueType: 1,
		}},
	}, nil
}

func deleteParser(tr *TokenReader) (*SynatxTreeNode, error) {
	t := tr.read()
	if t.typ != KeyWord || t.lit != "delete" {
		return nil, errors.New("You have a syntax error near: " + t.lit)
	}
	tKey := tr.read()
	if tKey.typ != Identifier {
		return nil, errors.New("You have a syntax error near: " + tKey.lit)
	}
	return &SynatxTreeNode{
		Name:      "delete",
		Child:     []*SynatxTreeNode{&SynatxTreeNode{
			Name:      "key",
			Value:     tKey.lit,
			ValueType: 0,
		}},
	}, nil
}

func valueParser(tr *TokenReader) (*SynatxTreeNode, error) {
	panic("valueParser")
}