package index

import (
	"fmt"
	"os"
	"testing"
)

func TestTree_Delete(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	TestTree_Insert(t)
	if tree, err = NewTree(); err != nil {
		t.Errorf("build tree error: %s", err)
	}
	defer tree.Close()
	cases := []struct{
		name string
		key  OFFTYPE
	} {
		{name:"delete 8", key: 8},
		{name:"delete 4", key: 4},
		{name:"delete 9", key: 9},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err = tree.delete(c.key); err != nil {
				t.Errorf("dlete key:%d error: %s", c.key, err)
			}
			if _, err = tree.find(c.key); err != nil {
				fmt.Printf("find key:%d error: %s", c.key, err)
			}
		})
	}
}

func TestTree_Update(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	if tree, err = NewTree(); err != nil {
		t.Errorf("build tree error: %s", err)
	}
	defer tree.Close()
	cases := []struct{
		name string
		key  OFFTYPE
		val  string
	} {
		{name:"update 1", key: 1, val: "1 + 1 = 2"},
		{name:"update 2", key: 2, val: "2 + 2 = 3"},
		{name:"update 3", key: 3, val: "3 + 3 = 6"},
		{name:"update 4", key: 4, val: "4 + 4 = 8"},
		{name:"update 5", key: 5, val: "5 + 5 = 10"},
		{name:"update 11", key: 11, val: "11 + 11 = 22"},
		//{name:"update 13", key: 13, val: "13"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err = tree.update(c.key, c.val); err != nil {
				t.Errorf("update key:%d error: %s", c.key, err)
			}
		})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ret string
			if ret, err = tree.find(c.key); err != nil {
				t.Errorf("find key:%d error: %s", c.key, err)
			}
			if ret != c.val {
				t.Errorf("get {%s}, but need {%s}", ret, c.val)
			}
		})
	}
}

func TestTree_Find(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	if tree, err = NewTree(); err != nil {
		t.Errorf("build tree error: %s", err)
	}
	defer tree.Close()
	cases := []struct{
		name string
		key  OFFTYPE
		ans  string
	} {
		{name:"find 1", key: 1, ans: "1 + 1"},
		{name:"find 2", key: 2, ans: "2 + 2"},
		{name:"find 3", key: 3, ans: "3 + 3"},
		//{name:"find 4", key: 4, ans: "4 + 4"},
		{name:"find 5", key: 5, ans: "5 + 5"},
		{name:"find 6", key: 6, ans: "6 + 6"},
		{name:"find 7", key: 7, ans: "7 + 7"},
		//{name:"find 8", key: 8, ans: "8 + 8"},
		//{name:"find 9", key: 9, ans: "9 + 9"},
		{name:"find 10", key: 10, ans: "10 + 10"},
		{name:"find 11", key: 11, ans: "11 + 11"},
		//{name:"find 12", key: 12, ans: "12 + 12"},
		//{name:"find 13", key: 13, ans: "13 + 13"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var ret string
			if ret, err = tree.find(c.key); err != nil {
				t.Errorf("find key:%d error: %s", c.key, err)
			}
			if ret != c.ans {
				t.Errorf("get {%s}, but need {%s}", ret, c.ans)
			}
		})
	}
}

func TestTree_Insert(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	clearStore()
	cases := []struct{
		name string
		key OFFTYPE
		val string
	} {
		{name:"insert 1", key: 1, val: "1 + 1"},
		{name:"insert 2", key: 2, val: "2 + 2"},
		{name:"insert 3", key: 3, val: "3 + 3"},
		{name:"insert 4", key: 4, val: "4 + 4"},
		{name:"insert 5", key: 5, val: "5 + 5"},
		{name:"insert 6", key: 6, val: "6 + 6"},
		{name:"insert 7", key: 7, val: "7 + 7"},
		{name:"insert 8", key: 8, val: "8 + 8"},
		{name:"insert 9", key: 9, val: "9 + 9"},
		{name:"insert 10", key: 10, val: "10 + 10"},
		{name:"insert 11", key: 11, val: "11 + 11"},
		//{name:"insert 12", key: 12, val: "12 + 12"},
		//{name:"insert 13", key: 13, val: "13 + 13"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if tree, err = NewTree(); err != nil {
				t.Errorf("build tree error: %s", err)
			}
			defer tree.Close()
			if err = tree.insert(c.key, c.val); err != nil {
				t.Errorf("insert{key:%d, ans: %s} error: %s", c.key, c.val, err)
			}
		})
	}
}

func clearStore() {
	if Exists(IndexFile) {
		os.Remove(IndexFile)
	}
	if Exists(DBFile) {
		os.Remove(DBFile)
	}
}

func Exists(path string) bool {
	_, err := os.Stat(path)    //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}