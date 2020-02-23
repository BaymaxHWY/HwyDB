package index

import (
	"os"
	"testing"
)

func TestTree_Find(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	if tree, err = NewTree(); err != nil {
		t.Errorf("build tree error: %s", err)
	}
	cases := []struct{
		name string
		key  OFFTYPE
		ans  string
	} {
		{name:"find 1", key: 1, ans: "1 + 1"},
		{name:"find 2", key: 2, ans: "2 + 2"},
		{name:"find 3", key: 3, ans: "3 + 3"},
		{name:"find 4", key: 4, ans: "4 + 4"},
		{name:"find 5", key: 5, ans: "5 + 5"},
		{name:"find 6", key: 6, ans: "6 + 6"},
		{name:"find 7", key: 7, ans: "7 + 7"},
		{name:"find 8", key: 8, ans: "8 + 8"},
		{name:"find 9", key: 9, ans: "9 + 9"},
		{name:"find 10", key: 10, ans: "10 + 10"},
		{name:"find 11", key: 11, ans: "11 + 11"},
		{name:"find 12", key: 12, ans: "12 + 12"},
		{name:"find 13", key: 13, ans: "13 + 13"},
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
		{name:"insert 12", key: 12, val: "12 + 12"},
		{name:"insert 13", key: 13, val: "13 + 13"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if tree, err = NewTree(); err != nil {
				t.Errorf("build tree error: %s", err)
			}
			if err = tree.insert(c.key, c.val); err != nil {
				t.Errorf("insert{key:%d, ans: %s} error: %s", c.key, c.val, err)
			}
			tree.Close()
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

//func TestBtree_Update(t *testing.T) {
//	bt := buildTree()
//	existKeys := []int64{6, 13, 1}
//	for _, key := range existKeys {
//		fmt.Printf("update: key:%v\n", key)
//		bt.Update(key, key+1)
//		if bt.Find(key) != key + 1 {
//			t.Fatal("update error")
//		}
//	}
//	walkBtree(bt.root)
//}
//
//func TestBtree_Delete(t *testing.T) {
//	bt := buildTree()
//	existKeys := []int64{6, 13, 1}
//	for _, key := range existKeys {
//		fmt.Printf("delete: key:%v\n", key)
//		bt.Delete(key)
//		if bt.Find(key) != nil {
//			t.Fatal("delete error")
//		}
//	}
//	walkBtree(bt.root)
//}
//
//func TestBtree_Find(t *testing.T) {
//	bt := buildTree()
//	existKeys := []int64{6, 13, 1, 4}
//	for _, key := range existKeys {
//		fmt.Printf("find: key:%v, value:%v\n", key, bt.Find(key))
//	}
//}
//
//func TestBtree_Insert(t *testing.T) {
//	bt := newBtree(3)
//	keys := []int32{1,2,3,5,6,8,9,11,13,15}
//	for _, key := range keys {
//		bt.Insert(key, key)
//		fmt.Println("key:", key)
//		walkBtree(bt.root)
//	}
//}
//
//func buildTree() *Btree {
//	bt := newBtree(3)
//	keys := []int64{1,2,3,5,6,8,9,11,13,15}
//	for _, key := range keys {
//		bt.Insert(key, key)
//	}
//	fmt.Println("init:")
//	walkBtree(bt.root)
//	return bt
//}
//
//func walkBtree(root *BNode) {
//	if root == nil {
//		return
//	}
//	level := 0
//	queue := make([]*BNode, 0)
//	queue = append(queue, root)
//	n := len(root.nodes)
//	nextN:= 0
//	fmt.Print(level, "---")
//	for len(queue) > 0 {
//		front := queue[:1]
//		queue = queue[1:]
//		for _, sn := range front[0].nodes {
//			fmt.Printf(" %v ", sn.key)
//			if front[0].isLeaf {
//				fmt.Printf(" %v }", sn.value)
//			}
//			if sn.childPtr != nil {
//				queue = append(queue, sn.childPtr)
//				nextN += len(sn.childPtr.nodes)
//			}
//			n--;
//		}
//		fmt.Print("||")
//		if n == 0 {
//			fmt.Println()
//			level++
//			fmt.Print(level, "---")
//			n = nextN
//			nextN = 0
//		}
//	}
//}