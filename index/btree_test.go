package index

import (
	"fmt"
	"testing"
)

func TestBtree_Update(t *testing.T) {
	bt := buildTree()
	existKeys := []int64{6, 13, 1}
	for _, key := range existKeys {
		fmt.Printf("update: key:%v\n", key)
		bt.Update(key, key+1)
		if bt.Find(key) != key + 1 {
			t.Fatal("update error")
		}
	}
	walkBtree(bt.root)
}

func TestBtree_Delete(t *testing.T) {
	bt := buildTree()
	existKeys := []int64{6, 13, 1}
	for _, key := range existKeys {
		fmt.Printf("delete: key:%v\n", key)
		bt.Delete(key)
		if bt.Find(key) != nil {
			t.Fatal("delete error")
		}
	}
	walkBtree(bt.root)
}

func TestBtree_Find(t *testing.T) {
	bt := buildTree()
	existKeys := []int64{6, 13, 1, 4}
	for _, key := range existKeys {
		fmt.Printf("find: key:%v, value:%v\n", key, bt.Find(key))
	}
}

func TestBtree_Insert(t *testing.T) {
	bt := newBtree(3)
	keys := []int32{1,2,3,5,6,8,9,11,13,15}
	for _, key := range keys {
		bt.Insert(key, key)
		fmt.Println("key:", key)
		walkBtree(bt.root)
	}
}

func buildTree() *Btree {
	bt := newBtree(3)
	keys := []int64{1,2,3,5,6,8,9,11,13,15}
	for _, key := range keys {
		bt.Insert(key, key)
	}
	fmt.Println("init:")
	walkBtree(bt.root)
	return bt
}

func walkBtree(root *BNode) {
	if root == nil {
		return
	}
	level := 0
	queue := make([]*BNode, 0)
	queue = append(queue, root)
	n := len(root.nodes)
	nextN:= 0
	fmt.Print(level, "---")
	for len(queue) > 0 {
		front := queue[:1]
		queue = queue[1:]
		for _, sn := range front[0].nodes {
			fmt.Printf(" %v ", sn.key)
			if front[0].isLeaf {
				fmt.Printf(" %v }", sn.value)
			}
			if sn.childPtr != nil {
				queue = append(queue, sn.childPtr)
				nextN += len(sn.childPtr.nodes)
			}
			n--;
		}
		fmt.Print("||")
		if n == 0 {
			fmt.Println()
			level++
			fmt.Print(level, "---")
			n = nextN
			nextN = 0
		}
	}
}