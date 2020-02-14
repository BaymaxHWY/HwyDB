package index

import (
	"errors"
	"fmt"
	"log"
)

const (
	NORMAL int = iota // normal
	SPLIT // split 分裂
	MERGE // merge 合并
	GREATER // greater 大于
	LESS // less 小于
	EQUAL // equal 等于
	LEQUAL // 小于等于
	GEQUAL // 大于等于
	//
	M = 3 // 阶数
)

type Btree struct {
	m int
	root *BNode
	sqt *BNode
}
//TODO:
// 1.索引节点和叶子节点的 record 应该不同，索引节点不应该存value
// 2.错误处理需要抽象出来
func NewBtree(m int) *Btree {
	bt := &Btree{m: m}
	root := NewBNode(true, m, nil, nil, 0)
	bt.root = root
	bt.sqt = root
	return bt
}
// 从根节点开始随机查找，查找到叶子节点才会结束
func (bt *Btree) findByRoot(inRecord Record) (*SNode) {
	bn, i := bt.root.findBNode(inRecord)
	if recordCompare(bn.nodes[i].record, inRecord, EQUAL) {
		return bn.nodes[i]
	}
	return nil
}
// 从最小关键字叶子节点开始顺序查找
func (bt *Btree) findBySqt(inRecord Record) (*SNode) {
	bn, idx := bt.sqt.findLeafBNode(inRecord)
	if bn == nil { // 说明sqt不是叶子结点，需要更改
		return nil
	}
	if !recordCompare(bn.nodes[idx].record, inRecord, EQUAL) {
		return nil
	}
	return bn.nodes[idx]
}
// 插入关键字
func (bt *Btree) insert(inRecord Record) {
	_, err := bt.insertRecursive(inRecord, nil, bt.root)
	if err != nil {
		log.Printf("insert failure %s", err)
	}
}
// 递归插入关键字
func (bt *Btree) insertRecursive(inRecord Record, parent, cur *BNode) (int, error) {
	idx := cur.binaryFind(inRecord)
	if cur.isLeaf {
		isUpdate, err := cur.insertElement(idx, NewSNode(inRecord, nil))
		if err != nil {
			return NORMAL, err
		}
		if isUpdate {
			bt.updateIndex(cur.nodes[idx].record, inRecord, cur.degree)
		}
		state := cur.checkBNode(cur == bt.root)
		if state == SPLIT {
			return cur.splitBNode(bt, parent), nil
		}
		return NORMAL, nil
	}
	state, err := bt.insertRecursive(inRecord, cur, cur.nodes[idx].childPtr)
	if err != nil {
		return NORMAL, err
	}
	if state == SPLIT {
		return cur.splitBNode(bt, parent), nil
	}
	return NORMAL, nil
}
// 删除关键字
func (bt *Btree) delete(inRecord Record) {
	_, err := bt.deleteRecursive(inRecord, nil, bt.root)
	if err != nil {
		log.Printf("delete failure %s", err)
	}
}
// 递归删除关键字
func (bt *Btree) deleteRecursive(inRecord Record, parent, cur *BNode) (int, error) {
	idx := cur.binaryFind(inRecord)
	if cur.isLeaf {
		if !recordCompare(cur.nodes[idx].record, inRecord, EQUAL) {
			return -1, errors.New("this key is not exist")
		}
		isUpdate, err := cur.deleteElement(idx)
		if err != nil {
			return NORMAL, err
		}
		if isUpdate {
			// 更新索引节点，把久的索引（本次删除的）换成新的（删除后剩下最大关键字）
			bt.updateIndex(inRecord, cur.nodes[len(cur.nodes)-1].record, cur.degree)
		}
		state := cur.checkBNode(cur == bt.root)
		if state == MERGE {
			return cur.mergeBNode(bt ,parent), nil
		}
		return NORMAL, nil
	}
	state, err := bt.deleteRecursive(inRecord, cur, cur.nodes[idx].childPtr)
	if err != nil {
		return NORMAL, err
	}
	if state == MERGE {
		return cur.mergeBNode(bt, parent), nil
	}
	return NORMAL, nil
}
// 更新操作
func (bt *Btree) update(inRecord Record) {
	// 找到叶子节点的关键字，更新值
	node := bt.findBySqt(inRecord)
	if node == nil {
		fmt.Println("update failure ", inRecord)
	} else {
		node.record.SetValue(inRecord.GetValue())
	}
}
// 在插入操作时，如果插入的新关键字最为最大（最小）关键字，则需要从root节点开始进行更新索引(指定深度degree)
// 仅修改 record，不改变指针
func (bt *Btree) updateIndex(oldIndex, newIndex Record, degree int) {
	cur := bt.root
	for cur.degree > degree {
		idx := cur.binaryFind(oldIndex)
		if recordCompare(cur.nodes[idx].record, oldIndex, EQUAL) {
			cur.nodes[idx].record = newIndex
		}
		cur = cur.nodes[idx].childPtr
	}
}

type BNode struct {
	isLeaf bool
	m int // 阶数
	nodes []*SNode
	next *BNode
	degree int // 节点所处的树的高度，叶子节点为0，root最高
}

func NewBNode(isLeaf bool, m int, nodes []*SNode, next *BNode, degree int) *BNode {
	return &BNode{isLeaf: isLeaf, m: m, nodes: nodes, next: next, degree:degree}
}
// 在该节点进行顺序查找（二分查找）
// 返回找到的SNode的序号
func (bn *BNode) binaryFind(inRecord Record) (int) {
	if len(bn.nodes) == 0 {
		return 0
	}
	left := 0
	right := len(bn.nodes) - 1
	for left < right {
		mid := left + (right - left) / 2
		if recordCompare(bn.nodes[mid].record, inRecord, EQUAL) {
			return mid
		}
		if recordCompare(bn.nodes[mid].record, inRecord, GREATER) {
			right = mid
		}else {
			left = mid + 1
		}
	}
	return left
}
// 递归查找BNode直达叶子节点
func (bn *BNode) findBNode(inRecord Record) (*BNode, int) {
	// 边界
	idx := bn.binaryFind(inRecord)
	if bn.isLeaf {
		return bn, idx
	}
	// 搜索
	return bn.nodes[idx].childPtr.findBNode(inRecord)
}
// 顺序查找从该叶子节点顺序查找
func (bn *BNode) findLeafBNode(inRecord Record) (*BNode, int) {
	if !bn.isLeaf {
		return nil, -1
	}
	for bn != nil {
		idx := bn.binaryFind(inRecord)
		if recordCompare(bn.nodes[idx].record, inRecord, GEQUAL) {
			return bn, idx;
		}
		bn = bn.next
	}
	return bn, len(bn.nodes)-1 // 如果inRecord比已经存在的关键字都大
}
// 插入元素
// return 是否需要更新索引节点
func (bn *BNode) insertElement(idx int, newSn *SNode) (bool, error) {
	if bn.nodes == nil {
		bn.nodes = []*SNode{NewSNode(newSn.record, newSn.childPtr)}
		return false, nil
	}
	res := newSn.record.Compare(bn.nodes[idx].record.Key())
	if res == 0 {
		return false, errors.New("key is exist")
	}
	if res > 0{
		bn.nodes = append(bn.nodes, newSn)
		return true, nil
	}
	bn.nodes = insertNodes(bn.nodes, idx, newSn)
	return false, nil
}
// 删除元素
// return 是否需要更新索引节点
func (bn *BNode) deleteElement(idx int) (bool, error) {
	bLen := len(bn.nodes)
	if bLen == 0 || idx >= bLen {
		return false, errors.New("the index is range out")
	}
	if idx == bLen - 1 {
		bn.nodes = bn.nodes[:idx]
		return true, nil

	}
	bn.nodes = append(bn.nodes[:idx], bn.nodes[idx+1:]...)
	return false, nil
}
// 查看兄弟节点是否还有多余位置(insert)
func (bn *BNode) hasFreePos(parent *BNode) (bool, *BNode, int) {
	if parent == nil {
		return  false, nil, -1
	}
	bkey := bn.nodes[len(bn.nodes)-1].record // 该节点在父节点的索引值（最大关键字）
	bidx := parent.binaryFind(bkey) // 在父节点的索引
	var rightNode, leftNode *BNode
	if bidx + 1 < len(parent.nodes) { // 右兄弟
		rightNode = parent.nodes[bidx+1].childPtr
		if len(rightNode.nodes) < bn.m {
			return true, rightNode, 0 // 可以插入右兄弟的位置
		}
	}
	if bidx - 1 >= 0 { // 左兄弟
		leftNode = parent.nodes[bidx-1].childPtr
		if len(leftNode.nodes) < bn.m {
			return true, leftNode, len(leftNode.nodes) - 1 // 可以插入右兄弟的位置
		}
	}
	return false, nil, -1 // 没有多余位置
}
// 询问兄弟节点是否还有多余关键字(delete)
func (bn *BNode) hasFreeKey(parent *BNode) (bool, *BNode, int) {
	if parent == nil {
		return false, nil, -1
	}
	bkey := bn.nodes[len(bn.nodes)-1].record // 该节点在父节点的索引值（最大关键字）
	bidx := parent.binaryFind(bkey) // 在父节点的索引
	var rightNode, leftNode *BNode
	if bidx + 1 < len(parent.nodes) { // 右兄弟
		rightNode = parent.nodes[bidx+1].childPtr
		if len(rightNode.nodes) > (bn.m + 1) / 2 {
			return true, rightNode, 0 // 第一个节点
		}
	}
	if bidx - 1 >= 0 { // 左兄弟
		leftNode = parent.nodes[bidx-1].childPtr
		if len(leftNode.nodes) > (bn.m + 1) / 2 {
			return true, leftNode, len(leftNode.nodes) - 1 // 最后一个节点
		}
	}
	if rightNode != nil {
		return false, rightNode, -1 // 左右兄弟都没有多余的key
	}
	if leftNode != nil {
		return false, leftNode, -1
	}
	return false, nil, -1 //如果没有兄弟节点，这种情况应该不存在，因为父节点一定时符合要求的，那么一定会有兄弟节点
}
// 检查该节点的关键字是否满足要求，不满足则进行对应的操作
func (bn *BNode) checkBNode(isRoot bool) int {
	count := len(bn.nodes)
	upLimit := bn.m
	lowerLimit := (bn.m + 1) >> 1
	if isRoot {
		lowerLimit = 1
	}
	if count > upLimit { // 超出限制，分裂
		return SPLIT
	}
	if count < lowerLimit { // 太少了，合并
		return MERGE
	}
	return NORMAL
}
// 分裂该节点（分裂之前要更新好索引节点的索引）
// 如果parent节点也需要分裂就返回 SPLIT 标记
func (bn *BNode) splitBNode(bt *Btree, parent *BNode) (int) {
	// 1. 先检查兄弟节点是否有空位置放
	// 2. 没有就分裂
	ok, brother, brohterIdx := bn.hasFreePos(parent)
	if ok {
		if brohterIdx == 0 { // 右兄弟，给该节点最大的关键字，本节点删除该关键字，更新索引
			insertNodes(brother.nodes, brohterIdx, bn.nodes[len(bn.nodes) - 1])
			bn.deleteElement(len(bn.nodes) - 1)
			bt.updateIndex(brother.nodes[0].record, bn.nodes[len(bn.nodes) - 1].record, bn.degree)
		} else { // 左兄弟，给该节点最小的关键字，本节点删除该关键字，更新索引
			brother.nodes = append(brother.nodes, bn.nodes[0])
			bn.deleteElement(0)
			bt.updateIndex(brother.nodes[brohterIdx].record, brother.nodes[brohterIdx+1].record, brother.degree)
		}
		return NORMAL
	}
	m := bn.m + 1
	leftNodes := make([]*SNode, 0, m>>1)
	rightNodes := make([]*SNode, 0, (m + 1)>>1)
	leftNodes = bn.nodes[:m>>1]
	rightNodes = bn.nodes[m>>1:]
	newBn := NewBNode(bn.isLeaf, bn.m, rightNodes, bn.next, bn.degree)
	bn.nodes = leftNodes
	if bn.isLeaf {
		bn.next = newBn
	}
	newSnL := NewSNode(bn.nodes[len(bn.nodes)-1].record, bn)
	if parent == nil { // 生成新的root节点
		newSnR := NewSNode(newBn.nodes[len(bn.nodes)-1].record, newBn)
		parent = NewBNode(false, bn.m, []*SNode{newSnL, newSnR}, nil, bn.degree+1)
		bt.root = parent // 更新
	} else {
		idx := parent.binaryFind(newSnL.record)
		for _, node := range parent.nodes { // 修改原parent节点指向bn的指针要指针新创建的节点newBn
			if node.childPtr == bn {
				node.childPtr = newBn
			}
		}
		parent.insertElement(idx, newSnL)
		return parent.checkBNode(parent == bt.root)
	}
	return NORMAL
}
// 合并节点（删除操作时）和兄弟节点合并
func (bn *BNode) mergeBNode(bt *Btree, parent *BNode) (int) {
	if parent == nil { // 可能和checkBNode有点重复
		return NORMAL // 单节点时删除不用检查
	}
	// 1.需要判断兄弟节点是否有多余关键字可以分配
	// 2.如果没有才进行合并
	ok, brother, brotherIdx := bn.hasFreeKey(parent)
	if ok {
		tmp := brother.nodes[brotherIdx]
		brother.deleteElement(brotherIdx) // 删除
		if brotherIdx == 0 { // 右兄弟
			bn.nodes = append(bn.nodes, tmp)
			bt.updateIndex(bn.nodes[len(bn.nodes)-2].record, tmp.record, bn.degree)
		} else { // 左兄弟
			bn.nodes = insertNodes(bn.nodes, 0, tmp)
			bt.updateIndex(tmp.record, brother.nodes[len(bn.nodes)-1].record, brother.degree)
		}
		return NORMAL
	}
	// 合并 brother 和 bn 节点 返回是否需要继续合并(需要注意更新索引节点)
	var left, right *BNode
	if recordCompare(bn.nodes[0].record, brother.nodes[0].record, GREATER) {
		left = brother
		right = bn
	} else {
		left = bn
		right = brother
	}
	oldIdx := len(left.nodes) - 1
	parent.deleteElement(parent.binaryFind(left.nodes[oldIdx].record)) // 删除left节点的最大关键字索引
	left.nodes = append(left.nodes, right.nodes...)
	right.nodes = left.nodes
	return parent.checkBNode(parent == bt.root)
}

type SNode struct {
	record Record
	childPtr *BNode
}

func NewSNode(record Record, childPtr *BNode) *SNode {
	return &SNode{record: record, childPtr: childPtr}
}

type Record interface {
	// key1.Compare(key2) > 0 ==> key1 > key2
	// key1.Compare(key2) = 0 ==> key1 = key2
	// key1.Compare(key2) < 0 ==> key1 < key2
	Compare(key interface{}) int64
	Key() interface{}
	SetValue(value interface{})
	GetValue() interface{}
}
// 根据need返回结果
// need = GREATER 如果r1 > r2 则返回 true
// need = LESS 如果r1 < r2 则返回 true
// need = EQUAL 如果r1 == r2 则返回 true
func recordCompare(r1, r2 Record, need int) bool {
	switch need {
	case GREATER:
		if r1.Compare(r2.Key()) > 0 {
			return true
		}
	case LESS:
		if r1.Compare(r2.Key()) < 0 {
			return true
		}
	case EQUAL:
		if r1.Compare(r2.Key()) == 0 {
			return true
		}
	case LEQUAL:
		if r1.Compare(r2.Key()) <= 0 {
			return true
		}
	case GEQUAL:
		if r1.Compare(r2.Key()) >= 0 {
			return true
		}
	}
	return false
}

// 向nodes数组中插入某个record
func insertNodes(nodes []*SNode, idx int, snode *SNode) []*SNode {
	tmp := append([]*SNode{}, nodes[idx:]...)
	nodes = append(nodes[:idx], snode)
	nodes = append(nodes, tmp...)
	return nodes
}