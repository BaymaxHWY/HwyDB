package index

import (
	"errors"
	"log"
)

const (
	Normal int = iota // normal
	Split             // split 分裂
	Merge             // merge 合并
)

type BT interface {
	Insert(key interface{}, value interface{})
	Find(key interface{}) (value interface{})
	Delete(key interface{})
	Update(key interface{}, value interface{})
}

func New(m int) BT {
	return newBtree(m)
}

type Btree struct {
	m int
	root *BNode
	sqt *BNode
}

func (bt *Btree) Insert(key interface{}, value interface{}) {
	input := typeToKey(key)
	bt.insert(input, value)
}

func (bt *Btree) Find(key interface{}) (value interface{}) {
	input := typeToKey(key)
	node := bt.findBySqt(input)
	if node == nil {
		return nil
	}
	return node.value
}

func (bt *Btree) Delete(key interface{}) {
	input := typeToKey(key)
	bt.delete(input)
}

func (bt *Btree) Update(key interface{}, value interface{}) {
	input := typeToKey(key)
	bt.update(input, value)
}

//TODO:
// 1.支持不同类型比较
// 2.错误处理
func newBtree(m int) *Btree {
	bt := &Btree{m: m}
	root := newBNode(true, m, nil, nil, 0)
	bt.root = root
	bt.sqt = root
	return bt
}
// 从根节点开始随机查找，查找到叶子节点才会结束
func (bt *Btree) findByRoot(key Key) (*SNode) {
	bn, i := bt.root.findBNode(key)
	if compare(bn.nodes[i].key, "=", key) {
		return bn.nodes[i]
	}
	return nil
}
// 从最小关键字叶子节点开始顺序查找
func (bt *Btree) findBySqt(key Key) (*SNode) {
	bn, idx := bt.sqt.findLeafBNode(key)
	if bn == nil { // 说明sqt不是叶子结点，需要更改
		return nil
	}
	if !compare(bn.nodes[idx].key, "=", key) {
		return nil
	}
	return bn.nodes[idx]
}
// 插入关键字
func (bt *Btree) insert(key Key, value interface{}) {
	_, err := bt.insertRecursive(key, nil, bt.root, value)
	if err != nil {
		log.Printf("insert failure %s", err)
	}
}
// 递归插入关键字
func (bt *Btree) insertRecursive(key Key, parent, cur *BNode, value interface{}) (int, error) {
	idx := cur.binaryFind(key)
	if cur.isLeaf {
		isUpdate, err := cur.insertElement(idx, newSNode(key, nil, value))
		if err != nil {
			return Normal, err
		}
		if isUpdate {
			bt.updateIndex(cur.nodes[idx].key, key, cur.degree)
		}
		state := cur.checkBNode(cur == bt.root)
		if state == Split {
			return cur.splitBNode(bt, parent), nil
		}
		return Normal, nil
	}
	state, err := bt.insertRecursive(key, cur, cur.nodes[idx].childPtr, value)
	if err != nil {
		return Normal, err
	}
	if state == Split {
		return cur.splitBNode(bt, parent), nil
	}
	return Normal, nil
}
// 删除关键字
func (bt *Btree) delete(key Key) {
	_, err := bt.deleteRecursive(key, nil, bt.root)
	if err != nil {
		log.Printf("delete failure %s", err)
	}
}
// 递归删除关键字
func (bt *Btree) deleteRecursive(key Key, parent, cur *BNode) (int, error) {
	idx := cur.binaryFind(key)
	if cur.isLeaf {
		if !compare(cur.nodes[idx].key,"=", key) {
			return -1, errors.New("this key is not exist")
		}
		isUpdate, err := cur.deleteElement(idx)
		if err != nil {
			return Normal, err
		}
		if isUpdate {
			// 更新索引节点，把久的索引（本次删除的）换成新的（删除后剩下最大关键字）
			bt.updateIndex(key, cur.nodes[len(cur.nodes)-1].key, cur.degree)
		}
		state := cur.checkBNode(cur == bt.root)
		if state == Merge {
			return cur.mergeBNode(bt ,parent), nil
		}
		return Normal, nil
	}
	state, err := bt.deleteRecursive(key, cur, cur.nodes[idx].childPtr)
	if err != nil {
		return Normal, err
	}
	if state == Merge {
		return cur.mergeBNode(bt, parent), nil
	}
	return Normal, nil
}
// 更新操作
func (bt *Btree) update(key Key, value interface{}) {
	// 找到叶子节点的关键字，更新值
	if bt.findBySqt(key) == nil {
		panic("the key is not exist")
	}
	node := bt.findBySqt(key)
	node.value = value
}
// 在插入操作时，如果插入的新关键字最为最大（最小）关键字，则需要从root节点开始进行更新索引(指定深度degree)
// 仅修改 key，不改变指针
func (bt *Btree) updateIndex(oldIndex, newIndex Key, degree int) {
	cur := bt.root
	for cur.degree > degree {
		idx := cur.binaryFind(oldIndex)
		if compare(cur.nodes[idx].key, "=",oldIndex) {
			cur.nodes[idx].key = newIndex
		}
		cur = cur.nodes[idx].childPtr
	}
}

type BNode struct {
	isLeaf bool
	m int // 阶数
	nodes []*SNode
	next *BNode // 叶子节点指向临近节点的指针
	degree int // 节点所处的树的高度，叶子节点为0，root最高
}

func newBNode(isLeaf bool, m int, nodes []*SNode, next *BNode, degree int) *BNode {
	return &BNode{isLeaf: isLeaf, m: m, nodes: nodes, next: next, degree:degree}
}
// 在该节点进行顺序查找（二分查找）
// 返回找到的SNode的序号
func (bn *BNode) binaryFind(key Key) (int) {
	if len(bn.nodes) == 0 {
		return 0
	}
	left := 0
	right := len(bn.nodes) - 1
	for left < right {
		mid := left + (right - left) / 2
		if compare(bn.nodes[mid].key, "=", key) {
			return mid
		}
		if compare(bn.nodes[mid].key, ">", key) {
			right = mid
		}else {
			left = mid + 1
		}
	}
	return left
}
// 递归查找BNode直达叶子节点
func (bn *BNode) findBNode(key Key) (*BNode, int) {
	// 边界
	idx := bn.binaryFind(key)
	if bn.isLeaf {
		return bn, idx
	}
	// 搜索
	return bn.nodes[idx].childPtr.findBNode(key)
}
// 顺序查找从该叶子节点顺序查找
func (bn *BNode) findLeafBNode(key Key) (*BNode, int) {
	if !bn.isLeaf {
		return nil, -1
	}
	for bn != nil {
		idx := bn.binaryFind(key)
		if compare(bn.nodes[idx].key, ">=", key) {
			return bn, idx;
		}
		bn = bn.next
	}
	if bn == nil {
		return bn, -1
	}
	return bn, len(bn.nodes)-1 // 如果inRecord比已经存在的关键字都大
}
// 插入元素
// return 是否需要更新索引节点
func (bn *BNode) insertElement(idx int, newSn *SNode) (bool, error) {
	if bn.nodes == nil {
		bn.nodes = []*SNode{newSNode(newSn.key, newSn.childPtr, newSn.value)}
		return false, nil
	}
	if compare(newSn.key, "=", bn.nodes[idx].key) {
		return false, errors.New("key is exist")
	}
	if compare(newSn.key, ">", bn.nodes[idx].key){
		bn.nodes = insertNodes(bn.nodes, idx+1, newSn)
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
	bkey := bn.nodes[len(bn.nodes)-1].key // 该节点在父节点的索引值（最大关键字）
	bidx := parent.binaryFind(bkey)       // 在父节点的索引
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
	bkey := bn.nodes[len(bn.nodes)-1].key // 该节点在父节点的索引值（最大关键字）
	bidx := parent.binaryFind(bkey)       // 在父节点的索引
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
		return Split
	}
	if count < lowerLimit { // 太少了，合并
		return Merge
	}
	return Normal
}
// 分裂该节点（分裂之前要更新好索引节点的索引）
// 如果parent节点也需要分裂就返回 Split 标记
func (bn *BNode) splitBNode(bt *Btree, parent *BNode) (int) {
	// 1. 先检查兄弟节点是否有空位置放
	// 2. 没有就分裂
	ok, brother, brohterIdx := bn.hasFreePos(parent)
	if ok {
		if brohterIdx == 0 { // 右兄弟，给该节点最大的关键字，本节点删除该关键字，更新索引
			insertNodes(brother.nodes, brohterIdx, bn.nodes[len(bn.nodes) - 1])
			bn.deleteElement(len(bn.nodes) - 1)
			bt.updateIndex(brother.nodes[0].key, bn.nodes[len(bn.nodes) - 1].key, bn.degree)
		} else { // 左兄弟，给该节点最小的关键字，本节点删除该关键字，更新索引
			brother.nodes = append(brother.nodes, bn.nodes[0])
			bn.deleteElement(0)
			bt.updateIndex(brother.nodes[brohterIdx].key, brother.nodes[brohterIdx+1].key, brother.degree)
		}
		return Normal
	}
	m := bn.m + 1
	leftNodes := make([]*SNode, 0, m>>1)
	rightNodes := make([]*SNode, 0, (m + 1)>>1)
	leftNodes = bn.nodes[:m>>1]
	rightNodes = bn.nodes[m>>1:]
	newBn := newBNode(bn.isLeaf, bn.m, rightNodes, bn.next, bn.degree)
	bn.nodes = leftNodes
	if bn.isLeaf {
		bn.next = newBn
	}
	var newSnL *SNode
	if bn.isLeaf {
		newSnL = newSNode(bn.nodes[len(bn.nodes)-1].key, bn, bn.nodes[len(bn.nodes)-1].value)
	} else {
		newSnL = newSNode(bn.nodes[len(bn.nodes)-1].key, bn, nil)
	}
	if parent == nil { // 生成新的root节点
		newSnR := newSNode(newBn.nodes[len(bn.nodes)-1].key, newBn, nil)
		parent = newBNode(false, bn.m, []*SNode{newSnL, newSnR}, nil, bn.degree+1)
		bt.root = parent // 更新
	} else {
		idx := parent.binaryFind(newSnL.key)
		for _, node := range parent.nodes { // 修改原parent节点指向bn的指针要指针新创建的节点newBn
			if node.childPtr == bn {
				node.childPtr = newBn
			}
		}
		parent.insertElement(idx, newSnL)
		return parent.checkBNode(parent == bt.root)
	}
	return Normal
}
// 合并节点（删除操作时）和兄弟节点合并
func (bn *BNode) mergeBNode(bt *Btree, parent *BNode) (int) {
	if parent == nil { // 可能和checkBNode有点重复
		return Normal // 单节点时删除不用检查
	}
	// 1.需要判断兄弟节点是否有多余关键字可以分配
	// 2.如果没有才进行合并
	ok, brother, brotherIdx := bn.hasFreeKey(parent)
	if ok {
		tmp := brother.nodes[brotherIdx]
		brother.deleteElement(brotherIdx) // 删除
		if brotherIdx == 0 { // 右兄弟
			bn.nodes = append(bn.nodes, tmp)
			bt.updateIndex(bn.nodes[len(bn.nodes)-2].key, tmp.key, bn.degree)
		} else { // 左兄弟
			bn.nodes = insertNodes(bn.nodes, 0, tmp)
			bt.updateIndex(tmp.key, brother.nodes[len(bn.nodes)-1].key, brother.degree)
		}
		return Normal
	}
	// 合并 brother 和 bn 节点 返回是否需要继续合并(需要注意更新索引节点)
	var left, right *BNode
	if compare(bn.nodes[0].key, ">", brother.nodes[0].key) {
		left = brother
		right = bn
	} else {
		left = bn
		right = brother
	}
	oldIdx := len(left.nodes) - 1
	parent.deleteElement(parent.binaryFind(left.nodes[oldIdx].key)) // 删除left节点的最大关键字索引
	left.nodes = append(left.nodes, right.nodes...)
	right.nodes = left.nodes
	return parent.checkBNode(parent == bt.root)
}

type Key interface {
	// 比较当前的key是否小于参数key
	// 如果 !a.Less(b) && !b.Less(a) ==> a == b
	Less(than Key) bool
}

type SNode struct {
	key      Key
	childPtr *BNode
	value    interface{} // 叶子小节点指向value的指针(或者值)
}

func newSNode(key Key, childPtr *BNode, value interface{}) *SNode {
	return &SNode{key: key, childPtr: childPtr, value:value}
}

// 依照运算符 op 对r1和r2进行比较运算
func compare(r1 Key, op string, r2 Key) bool {
	switch op {
	case ">":
		if r2.Less(r1) { // r1 > r2
			return true
		}
	case ">=":
		if !r1.Less(r2) { // r1 >= r2
			return true
		}
	case "<":
		if r1.Less(r2) { // r1 < r2
			return true
		}
	case "<=":
		if !r2.Less(r1) { // r1 <= r2
			return true
		}
	case "=":
		if !r1.Less(r2) && !r2.Less(r1) {
			return true
		}
	}
	return false
}

// 向nodes数组中插入某个record(位置为idx)，原idx位置的元素需要比插入元素小
func insertNodes(nodes []*SNode, idx int, snode *SNode) []*SNode {
	tmp := append([]*SNode{}, nodes[idx:]...)
	nodes = append(nodes[:idx], snode)
	nodes = append(nodes, tmp...)
	return nodes
}