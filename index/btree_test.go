package index

import (
	"fmt"
	"strconv"
	"testing"
)

type TestInsert struct {
	key int
	value string
}

func (t *TestInsert) String() string {
	return fmt.Sprintf("{key:%d, value:%s}", t.key, t.value)
}

func (t *TestInsert) Compare(key interface{}) int64 {
	return int64(t.key - key.(int))
}

func (t *TestInsert) Key() interface{} {
	return t.key
}

func (t *TestInsert) GetValue() interface{} {
	return t.value
}

func (t *TestInsert) SetValue(value interface{}) {
	t.value = value.(string)
}

func TestBtree_update(t *testing.T) {
	bt := buildTreeTest()
	deleteKeys := []int{40, 15, 85, 99, 300}
	for _, key := range deleteKeys {
		update := &TestInsert{
			key:   key,
			value: strconv.Itoa(key + -100),
		}
		bt.update(update)
		fmt.Println("update:", key)
		walkBtree(bt.root, 0)
	}
}

func TestBtree_delete(t *testing.T) {
	bt := buildTreeTest()
	deleteKeys := []int{40, 15, 85, 99, 300}
	for _, key := range deleteKeys {
		delete := &TestInsert{
			key:   key,
			value: strconv.Itoa(key),
		}
		bt.delete(delete)
		fmt.Println("delete:", key)
		walkBtree(bt.root, 0)
	}
}

func TestBtree_Insert(t *testing.T) {
	keys := []int{10, 15, 21, 37, 51, 59, 44, 63, 72, 85, 91, 97, 99, 100, 200, 300}
	bt := NewBtree(3)
	for _, key := range keys {
		input := &TestInsert{
			key:   key,
			value: strconv.Itoa(key),
		}
		bt.insert(input)
		fmt.Println(key, ":")
		walkBtree(bt.root, 0)
	}
}

func TestBtree_findBySqt(t *testing.T) {
	bt := buildTreeTest()
	existKeys := []int{37, 97, 10}
	noExistKeys := []int{41, 65}
	for _, key := range existKeys {
		node := bt.findBySqt(&TestInsert{key:key})
		if node == nil {
			t.Fatal(key, " test")
		}
	}
	for _, key := range noExistKeys {
		node := bt.findBySqt(&TestInsert{key:key})
		if node != nil {
			t.Fatal(key, " test")
		}
	}
}

func TestBtree_findByRoot(t *testing.T) {
	bt := buildTreeTest()
	existKeys := []int{37, 97, 10}
	noExistKeys := []int{41, 65}
	for _, key := range existKeys {
		node := bt.findByRoot(&TestInsert{key:key})
		if node == nil {
			t.Fatal(key, " test")
		}
	}
	for _, key := range noExistKeys {
		node := bt.findByRoot(&TestInsert{key:key})
		if node != nil {
			t.Fatal(key, " test")
		}
	}
}

func TestBNode_findBNode(t *testing.T) {
	bt := buildTreeTest()
	existKeys := []int{37, 97, 10}
	noExistKeys := []int{41, 65}
	for _, key := range existKeys {
		bn, idx := bt.root.findBNode(&TestInsert{key:key})
		if bn.nodes[idx].record.Key().(int) != key {
			t.Fatal(key, " test")
		}
	}
	trueAns := []int{44, 72}
	for i, key := range noExistKeys {
		bn, idx := bt.root.findBNode(&TestInsert{key:key})
		if bn.nodes[idx].record.Key().(int) != trueAns[i] {
			t.Fatal(key, " test")
		}
	}
}

func TestBNode_binaryFind(t *testing.T) {
	keys1 := []int{15, 44, 51, 59}
	keys2 := []int{15, 20, 44, 53, 59}
	target := 30
	var nodes []*SNode
	for _, key := range keys1 {
		nodes = append(nodes, &SNode{&TestInsert{key:key, value: strconv.Itoa(key)}, nil})
	}
	bn := NewBNode(true, 5, nodes, nil, 0)
	t.Logf("test1:\n")
	t.Logf("binary search : %d", bn.binaryFind(&TestInsert{target, strconv.Itoa(target)}))
	nodes = make([]*SNode, 0, len(keys2))
	for _, key := range keys2 {
		nodes = append(nodes, &SNode{&TestInsert{key:key, value: strconv.Itoa(key)}, nil})
	}
	bn = NewBNode(true, 5, nodes, nil, 0)
	t.Logf("test2:\n")
	t.Logf("binary search : %d", bn.binaryFind(&TestInsert{target, strconv.Itoa(target)}))
}

func buildTreeTest() *Btree {
	keys := []int{10, 15, 21, 37, 51, 59, 44, 63, 72, 85, 91, 97, 99, 100, 200, 300, 40}
	bt := NewBtree(3)
	for _, key := range keys {
		input := &TestInsert{
			key:   key,
			value: strconv.Itoa(key),
		}
		bt.insert(input)
	}
	fmt.Println("init tree:")
	walkBtree(bt.root, 0)
	return bt
}

func genNode(keys []int) []*SNode {
	nodes := make([]*SNode, 0, 3)
	for _, key := range keys {
		nodes = append(nodes, &SNode{&TestInsert{key, ""}, nil})
	}
	return nodes
}

func walkBtree(root *BNode, level int) {
	if root == nil {
		return
	}
	queue := make([]*BNode, 0)
	queue = append(queue, root)
	n := len(root.nodes)
	nextN:= 0
	for len(queue) > 0 {
		front := queue[:1]
		queue = queue[1:]
		for _, sn := range front[0].nodes {
			fmt.Printf("%d %s ", level, sn.record)
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
			n = nextN
			nextN = 0
		}
	}
}