package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

// TODO:重写b+tree，实现磁盘存储功能（持久化）
//  没有进行缓存优化的，没有设置文件大小限制

// 从文件中读取的流程：1.使用ReadAt方法从给定的offset处开始读取固定长度的buf；2.使用binary库将读取到的buf进行反序列化
// 把数据写入文件的流程：1.先使用binary库将struct进行序列化写入到buf；2.再使用WriteAt方法在给定offset写入固定长度的buf
const (
	M = 4 // B+tree's order
	INVAILD_OFFSET = 0xdeadbeef
)

var (
	BN     = (M + 1) / 2 // BALANCE_NUMBER ceil(M/2)，用于限制节点的kv键值对数量
	DBFile = "/Users/hwy/Code/Go/HwyDB/store/DB.db"          // 实际数据存储的文件
	IndexFile = "/Users/hwy/Code/Go/HwyDB/store/IndexFile.db" // 存储索引数据文件（前8个字节存储root节点的地址）
	NotFindKey = errors.New("the key is not found!")
	ExistKey = errors.New("the key is exist!")
	ReadHeadErr = errors.New("read IndexFile header error")
)

type OFFTYPE uint64

type Tree struct {
	root      OFFTYPE  // 根节点的偏移地址
	indexFile *os.File // indexFile文件句柄
	dbFile *os.File // DB文件句柄
	maxBytes  int64    // 分配给一个节点的最大存储空间(也就是每次分配空间的大小)
	fileSize  int64    // 文件大小，初始值为8（即第一个插入的Node的起始地址）或者读入文件的大小（文件不为空的话）
}

type Node struct {
	IsActivate bool // 判断当前Node是否处于使用状态
	IsLeaf   bool
	Self     OFFTYPE
	Next     OFFTYPE
	Parent   OFFTYPE
	Children []KV
}

type KV struct {
	Key OFFTYPE
	Val OFFTYPE // 1.指向子节点的偏移（内部节点） 2.指向存储数据文件的偏移（叶子结点）
}
// 初始化Tree
func NewTree() (*Tree, error) {
	// 读取文件、重建B+tree（如果文件不为空的话）
	var (
		err error
		fstat os.FileInfo
	)
	t := &Tree{}
	t.root = INVAILD_OFFSET
	t.maxBytes = int64(27 + 32 * BN)
	if t.indexFile, err = os.OpenFile(IndexFile, os.O_CREATE | os.O_RDWR, 0644); err != nil {
		return nil, err
	}
	if t.dbFile, err = os.OpenFile(DBFile, os.O_CREATE | os.O_RDWR, 0644); err != nil {
		return nil, err
	}
	if fstat, err = t.indexFile.Stat(); err != nil {
		return nil, err
	}
	t.fileSize = fstat.Size()
	if t.fileSize != 0 {
		// 读取文件，只需要读root就好
		buf := make([]byte, 8)
		n, err :=t.indexFile.ReadAt(buf, 0)
		if err != nil {
			return nil, err
		}
		if n != 8 {
			return nil, ReadHeadErr
		}
		bs := bytes.NewBuffer(buf)
		rootAdd := uint64(0)
		if err = binary.Read(bs, binary.LittleEndian, &rootAdd); err != nil {
			return nil, err
		}
		t.root = OFFTYPE(rootAdd)
	}else {
		// 初始化前八个字节
		t.fileSize = 8
		if err = t.flushTree(8); err != nil {
			return nil, err
		}
		//r := uint64(8)
		//bs := new(bytes.Buffer)
		//if err = binary.Write(bs, binary.LittleEndian, r); err != nil {
		//	return nil, err
		//}
		//if _, err := t.indexFile.WriteAt(bs.Bytes(), 0); err != nil {
		//	return nil, err
		//}
	}
	return t, nil
}
// 更新root（在IndexFile文件中）
func (t *Tree) flushTree(root_off OFFTYPE) error {
	var err error
	r := uint64(root_off)
	bs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(bs, binary.LittleEndian, r); err != nil {
		return err
	}
	if _, err := t.indexFile.WriteAt(bs.Bytes(), 0); err != nil {
		return err
	}
	return nil
}

func (t *Tree) Close() error {
	if t.indexFile != nil {
		t.indexFile.Sync()
		return t.indexFile.Close()
	}
	return nil
}

func (t *Tree) insert(key OFFTYPE, val string) error {
	// 1. 检查B+树是否为空
	// 2. 查找到所属的叶子结点
	var (
		node *Node
		err error
	)
	// 把value存在DB文件中
	valOff, err := t.saveValue(val)
	if err != nil {
		return err
	}
	insertKV := KV{
		Key: key,
		Val: valOff,
	}
	if t.root == INVAILD_OFFSET {
		// 树为空, 分配新的存储空间
		node = t.getNewDiskStore(true)
		node.Children = make([]KV, 0, M)
		node.Children = append(node.Children, insertKV)
		// 写入磁盘
		if err = t.flushNode(node); err != nil {
			return err
		}
		t.root = node.Self
		return nil
	}
	// 查找叶子节点
	if node, err = t.seekNode(t.root); err != nil {
		return err
	}
	for !node.IsLeaf {
		idx := sort.Search(len(node.Children), func(i int) bool {
			return node.Children[i].Key >= key
		})
		if idx >= len(node.Children) { // 如果target比所有子节点的key都大的话会返回len(node.Children) 防止越界
			idx = len(node.Children) - 1
		}
		if node, err = t.seekNode(node.Children[idx].Val); err != nil {
			return err
		}
	}
	// 对该叶子节点进行插入操作
	if _, err := t.insertKvToLeafNode(node, insertKV); err != nil {
		return err
	}
	// 判断是否需要分裂
	if len(node.Children) <= M {
		return t.flushNode(node)
	}
	// 分裂并写入磁盘
	new_node := t.getNewDiskStore(node.IsLeaf)
	if err := t.SplitLeafNode(node, new_node); err != nil {
		return err
	}
	if err := t.flushMultiNodes(node, new_node); err != nil {
		return err
	}
	return t.insertParentNode(node.Parent, node, new_node)
}
// 向parent节点插入kv
func (t *Tree) insertParentNode(parent_off OFFTYPE, left *Node, right *Node) error {
	// 判断parent节点是否存在
	var (
		parent *Node
		err error
	)
	if left == nil || right == nil {
		return fmt.Errorf("insertParentNode: one of input is nil")
	}
	if parent_off == INVAILD_OFFSET {
		// 不存在
		if err = t.genNewRootNode(left, right); err != nil {
			return err
		}
		return t.flushMultiNodes(left, right)
	}
	// parent存在
	if parent, err =t.seekNode(parent_off); err != nil {
		return err
	}
	newKv := left.Children[len(left.Children)-1]
	addKv := right.Children[len(right.Children)-1]
	newKv.Val = left.Self
	addKv.Val = right.Self
	idxL := sort.Search(len(parent.Children), func(i int) bool {
		return parent.Children[i].Val >= newKv.Val
	})
	// 更新索引节点
	if err = t.mayUpdateIndex(parent, newKv.Key, parent.Children[idxL].Key); err != nil {
		return err
	}
	parent.Children[idxL] = newKv
	// 将新节点插入
	if _, err = t.insertKvToNode(parent, addKv); err != nil {
		return err
	}
	if len(parent.Children) <= M {
		if err = t.flushNode(parent); err != nil {
			return err
		}
		return nil
	}
	// parent需要分裂
	new_parent := t.getNewDiskStore(parent.IsLeaf)
	if err = t.SplitInternalNode(parent, new_parent); err != nil {
		return err
	}
	if err = t.flushMultiNodes(parent, new_parent); err != nil {
		return err
	}
	return t.insertParentNode(parent.Parent, parent, new_parent)
}
// 生成一个root节点
func (t *Tree) genNewRootNode(left *Node, right *Node) error {
	var(
		kvL, kvR KV
		root *Node
	)
	if left == nil || right == nil {
		return fmt.Errorf("genNewRootNode: one of input is nil")
	}
	kvL = left.Children[len(left.Children)-1]
	kvR = right.Children[len(right.Children)-1]
	kvL.Val = left.Self
	kvR.Val = right.Self
	root = t.getNewDiskStore(false)
	root.Children = append([]KV{}, kvL, kvR)
	left.Parent = root.Self
	right.Parent = root.Self
	t.root = root.Self
	// 更新t.root，在硬盘上
	if err := t.flushTree(t.root); err != nil {
		return err
	}
	return t.flushNode(root) // 写入硬盘
}
// 向内部节点node插入kv
// return kv插入到children中的下标
func (t *Tree) insertKvToNode(node *Node, kv KV) (int, error) {
	if node == nil {
		return 0, fmt.Errorf("insertKvToNode: node is nil")
	}
	idx := sort.Search(len(node.Children), func(i int) bool {
		return node.Children[i].Key >= kv.Key
	})
	var oldKey OFFTYPE
	if idx == len(node.Children) {
		oldKey = node.Children[idx-1].Key
	}else {
		oldKey = node.Children[idx].Key
	}
	node.Children = append(node.Children, kv)
	for i := len(node.Children) - 1; i > idx; i-- { // idx+1~最后的kv都后移一位
		node.Children[i] = node.Children[i-1]
	}
	node.Children[idx] = kv
	// 向上更新索引（可能）
	if err := t.mayUpdateIndex(node, kv.Key, oldKey); err != nil {
		return -1, err
	}
	return idx, nil
}
// 向叶子节点node插入kv
// return kv插入到children中的下标
func (t *Tree) insertKvToLeafNode(node *Node, kv KV) (int ,error) {
	if node == nil {
		return 0, fmt.Errorf("insertKvToLeafNode: node is nil")
	}
	idx := sort.Search(len(node.Children), func(i int) bool {
		return node.Children[i].Key >= kv.Key
	})
	// 判断insert kv是否已经存在
	if idx < len(node.Children) && node.Children[idx].Key == kv.Key {
		return 0, ExistKey
	}
	var oldKey OFFTYPE
	if idx == len(node.Children) {
		oldKey = node.Children[idx-1].Key
	}else {
		oldKey = node.Children[idx].Key
	}
	node.Children = append(node.Children, kv)
	for i := len(node.Children) - 1; i > idx; i-- { // idx+1~最后的kv都后移一位
		node.Children[i] = node.Children[i-1]
	}
	node.Children[idx] = kv
	// 向上更新索引（可能）
	if err := t.mayUpdateIndex(node, kv.Key, oldKey); err != nil {
		return -1, err
	}
	return idx, nil
}
// 从node开始向上递归更新索引
func (t *Tree) mayUpdateIndex(node *Node, newKey, oldKey OFFTYPE) error {
	var (
		parent *Node
		err error
	)
	if node == nil {
		return nil
	}
	if node.Parent == INVAILD_OFFSET {
		return nil
	}
	if parent, err = t.seekNode(node.Parent); err != nil {
		return err
	}
	if idx := t.findKey(parent, oldKey); idx != -1 {
		parent.Children[idx].Key = newKey
		if err = t.flushNode(parent); err != nil {
			return err
		}
		return t.mayUpdateIndex(parent, newKey, oldKey)
	}
	return nil
}
// 在node中查询key
// 存在则返回索引，不存在则返回-1
func (t *Tree) findKey(node *Node, key OFFTYPE) int {
	if node == nil {
		return -1
	}
	idx := sort.Search(len(node.Children), func(i int) bool {
		return node.Children[i].Key >= key
	})
	if idx >= len(node.Children) || node.Children[idx].Key != key {
		return -1
	}
	return idx
}
// 分裂内部节点（非叶子结点）
func (t *Tree) SplitInternalNode(left, right *Node) error {
	if left == nil || right == nil {
		return fmt.Errorf("SplitLeafNode: one of input's nodes is nil")
	}
	split := (M + 1) / 2
	right.Children = append([]KV{}, left.Children[split:]...)
	left.Children = left.Children[:split]
	right.Parent = left.Parent
	right.IsLeaf = false
	// 需要更新新节点（right）的所属子节点的parent
	for _, c := range right.Children {
		if child, err := t.seekNode(c.Val); err != nil {
			return err
		}else {
			child.Parent = right.Self
			if err = t.flushNode(child); err != nil {
				return err
			}
		}
	}
	return nil
}
// 分裂叶子结点
func (t *Tree) SplitLeafNode(left, right *Node) error {
	// 将left节点分裂成left和right两个节点
	if left == nil || right == nil {
		return fmt.Errorf("SplitLeafNode: one of input's nodes is nil")
	}
	split := (M + 1) / 2
	right.Children = append([]KV{}, left.Children[split:]...)
	left.Children = left.Children[:split]
	left.Next = right.Self
	right.Parent = left.Parent
	right.IsLeaf = true
	return nil
}

func (t *Tree) initNode(node *Node) {
	if node == nil {
		fmt.Println("initNode: this node need statement")
		return
	}
	node.IsActivate = false
	node.IsLeaf = false
	node.Next = INVAILD_OFFSET
	node.Self = INVAILD_OFFSET
	node.Parent = INVAILD_OFFSET
}

func (t *Tree) find(target OFFTYPE) (string, error) {
	var (
		node *Node
		err error
	)
	if t.root == INVAILD_OFFSET { // B+树是空的
		return "", nil
	}
	if node, err = t.seekNode(t.root); err != nil { // 获得root节点
		return "", err
	}
	for !node.IsLeaf { // 找到叶子结点
		idx := sort.Search(len(node.Children), func(i int) bool {
			return node.Children[i].Key >= target
		})
		if idx >= len(node.Children) { // 如果target比所有子节点的key都大的话会返回len(node.Children) 防止越界
			idx = len(node.Children) - 1
		}
		if node, err = t.seekNode(node.Children[idx].Val); err != nil {
			return "nil", err
		}
	}
	for _, child := range node.Children {
		if child.Key == target { // 找到
			return t.getValue(child.Val)
		}
	}
	return "", NotFindKey
}
// 根据offset生成Node结构(结果存储在node中)
func (t *Tree) seekNode(offset OFFTYPE) (*Node, error) {
	// 从文件中读取地址起点为offset，终点为offset + maxBytes之间存储的值，并赋值给node
	// 需要检查offset的合法性
	node := &Node{}
	if offset == INVAILD_OFFSET {
		// 初始化node
		t.initNode(node)
		return node, nil
	}
	// 读取文件
	buf := make([]byte, t.maxBytes)
	if _, err := t.indexFile.ReadAt(buf, int64(offset)); err != nil && err != io.EOF {
		return nil, err
	}
	// 字节反序列化为对象
	bs := bytes.NewBuffer(buf)
	// IsActivate
	if err := binary.Read(bs, binary.LittleEndian, &node.IsActivate); err != nil {
		return nil, err
	}
	// IsLeaf
	if err := binary.Read(bs, binary.LittleEndian, &node.IsLeaf); err != nil {
		return nil, err
	}
	// Self
	if err := binary.Read(bs, binary.LittleEndian, &node.Self); err != nil {
		return nil, err
	}
	// Next
	if err := binary.Read(bs, binary.LittleEndian, &node.Next); err != nil {
		return nil, err
	}
	// Parent
	if err := binary.Read(bs, binary.LittleEndian, &node.Parent); err != nil {
		return nil, err
	}
	// Children
	kvLen := uint8(0)
	if err := binary.Read(bs, binary.LittleEndian, &kvLen); err != nil {
		return nil, err
	}
	for i := uint8(0); i < kvLen; i++ {
		child := KV{}
		if err := binary.Read(bs, binary.LittleEndian, &child); err != nil {
			return nil, err
		}
		node.Children = append(node.Children, child)
	}
	return node, nil
}
// 从DB文件根据给定的offset获得value值（string类型）
func (t *Tree) getValue(offset OFFTYPE) (string, error) {
	var (
		err error
	)
	if t.dbFile == nil {
		return "", fmt.Errorf("db file is not exist!")
	}
	// 获取value存储所用字节长度
	buf := make([]byte, 1)
	if _, err := t.dbFile.ReadAt(buf, int64(offset)); err != nil {
		return "", err
	}
	bs := bytes.NewBuffer(buf)
	dataLen := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return "", err
	}
	// 读取value
	buf = make([]byte, dataLen)
	if _, err := t.dbFile.ReadAt(buf, int64(offset + 1)); err != nil {
		return "", err
	}
	bs = bytes.NewBuffer(buf)
	retBuf := make([]byte, dataLen)
	for i := uint8(0); i < dataLen; i++ {
		if err = binary.Read(bs, binary.LittleEndian, &retBuf[i]); err != nil {
			return "", err
		}
	}
	return string(retBuf[:]), nil
}
// 将value存储在DB文件中（在文件末尾增加）
func (t *Tree) saveValue(val string) (OFFTYPE, error) {
	// 获取文件句柄
	var (
		err error
		ret int64
		fstat os.FileInfo
	)
	if t.dbFile == nil {
		if t.dbFile, err = os.OpenFile(DBFile, os.O_CREATE | os.O_RDWR, 0644); err != nil {
			return INVAILD_OFFSET, err
		}
	}
	if fstat, err = t.dbFile.Stat(); err != nil {
		return INVAILD_OFFSET, err
	}
	ret = fstat.Size()
	// 获取val的字节数并序列化
	valBuf := []byte(val)
	valen := uint8(len(valBuf))
	bs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(bs, binary.LittleEndian, valen); err != nil {
		return INVAILD_OFFSET, err
	}
	// 把val序列化
	if err = binary.Write(bs, binary.LittleEndian, valBuf); err != nil {
		return INVAILD_OFFSET, err
	}
	// 把序列化的结果存储进文件
	if _, err = t.dbFile.WriteAt(bs.Bytes(), ret); err != nil {
		return INVAILD_OFFSET, err
	}
	return OFFTYPE(ret), nil
}
// TODO:更新value
func (t *Tree) updateValue(val_off OFFTYPE, new_val string) error {
	panic("updateValue")
}
// 将多个node的信息写入磁盘
func (t *Tree) flushMultiNodes(nodes... *Node) error {
	for _, node := range nodes {
		if err := t.flushNode(node); err != nil {
			return err
		}
	}
	return nil
}
// 将node的信息写入磁盘
func (t *Tree) flushNode(node *Node) (error) {
	if node == nil {
		return fmt.Errorf("flushNode: flush failed, input is nil")
	}
	if t.indexFile == nil {
		return fmt.Errorf("flush node into disk, but not open indexFile")
	}
	var (
		err error
		length int
	)
	// 对象序列化
	bs := bytes.NewBuffer(make([]byte, 0))
	// IsActivate
	if err = binary.Write(bs, binary.LittleEndian, node.IsActivate); err != nil {
		return err
	}
	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, node.IsLeaf); err != nil {
		return err
	}
	// Self
	if err = binary.Write(bs, binary.LittleEndian, node.Self); err != nil {
		return err
	}
	// Next
	if err = binary.Write(bs, binary.LittleEndian, node.Next); err != nil {
		return err
	}
	// Parent
	if err = binary.Write(bs, binary.LittleEndian, &node.Parent); err != nil {
		return err
	}
	// Children
	childCount := uint8(len(node.Children))
	// Children' length
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for i := uint8(0); i < childCount; i++ {
		if err = binary.Write(bs, binary.LittleEndian, node.Children[i]); err != nil {
			return err
		}
	}
	// 写入多余的child占用着
	for i := uint8(2 * BN); i > childCount; i-- {
		if err = binary.Write(bs, binary.LittleEndian, KV{Key:INVAILD_OFFSET, Val:INVAILD_OFFSET}); err != nil {
			return err
		}
	}
	// 写入文件
	if length, err = t.indexFile.WriteAt(bs.Bytes(), int64(node.Self)); err != nil {
		return err
	} else if len(bs.Bytes()) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(node.Self), t.indexFile.Name(), len(bs.Bytes()), length)
	}
	//fmt.Println("node size:", length)
	return nil
}
// 分配一块新的存储空间(逻辑上)
func (t *Tree) getNewDiskStore(isLeaf bool) (*Node) {
	node := &Node{}
	t.initNode(node)
	node.IsLeaf = isLeaf
	node.IsActivate = true
	node.Self = OFFTYPE(t.fileSize)
	t.fileSize += t.maxBytes
	return node
}

//
//import (
//	"errors"
//	"log"
//)
//
//
//
//const (
//	Normal int = iota // normal
//	Split             // split 分裂
//	Merge             // merge 合并
//	//
//	BN int64 = 3 // BALANCE_NUMBER
//)
//
//type BT interface {
//	Insert(key interface{}, value interface{})
//	Find(key interface{}) (value interface{})
//	Delete(key interface{})
//	Update(key interface{}, value interface{})
//}
//
//func New(m int) BT {
//
//	return newBtree(m)
//}
//
//type Btree struct {
//	m int
//	root *BNode
//	sqt *BNode
//}
//
//func (bt *Btree) Insert(key interface{}, value interface{}) {
//	input := typeToKey(key)
//	bt.insert(input, value)
//}
//
//func (bt *Btree) Find(key interface{}) (value interface{}) {
//	input := typeToKey(key)
//	node := bt.findBySqt(input)
//	if node == nil {
//		return nil
//	}
//	return node.value
//}
//
//func (bt *Btree) Delete(key interface{}) {
//	input := typeToKey(key)
//	bt.delete(input)
//}
//
//func (bt *Btree) Update(key interface{}, value interface{}) {
//	input := typeToKey(key)
//	bt.update(input, value)
//}
//
//
//// 1.支持不同类型比较
//// 2.错误处理
//func newBtree(m int) *Btree {
//	bt := &Btree{m: m}
//	root := newBNode(true, m, nil, nil, 0)
//	bt.root = root
//	bt.sqt = root
//	return bt
//}
//// 从根节点开始随机查找，查找到叶子节点才会结束
//func (bt *Btree) findByRoot(key Key) (*SNode) {
//	bn, i := bt.root.findBNode(key)
//	if compare(bn.nodes[i].key, "=", key) {
//		return bn.nodes[i]
//	}
//	return nil
//}
//// 从最小关键字叶子节点开始顺序查找
//func (bt *Btree) findBySqt(key Key) (*SNode) {
//	bn, idx := bt.sqt.findLeafBNode(key)
//	if bn == nil { // 说明sqt不是叶子结点，需要更改
//		return nil
//	}
//	if !compare(bn.nodes[idx].key, "=", key) {
//		return nil
//	}
//	return bn.nodes[idx]
//}
//// 插入关键字
//func (bt *Btree) insert(key Key, value interface{}) {
//	_, err := bt.insertRecursive(key, nil, bt.root, value)
//	if err != nil {
//		log.Printf("insert failure %s", err)
//	}
//}
//// 递归插入关键字
//func (bt *Btree) insertRecursive(key Key, parent, cur *BNode, value interface{}) (int, error) {
//	idx := cur.binaryFind(key)
//	if cur.isLeaf {
//		isUpdate, err := cur.insertElement(idx, newSNode(key, nil, value))
//		if err != nil {
//			return Normal, err
//		}
//		if isUpdate {
//			bt.updateIndex(cur.nodes[idx].key, key, cur.degree)
//		}
//		state := cur.checkBNode(cur == bt.root)
//		if state == Split {
//			return cur.splitBNode(bt, parent), nil
//		}
//		return Normal, nil
//	}
//	state, err := bt.insertRecursive(key, cur, cur.nodes[idx].childPtr, value)
//	if err != nil {
//		return Normal, err
//	}
//	if state == Split {
//		return cur.splitBNode(bt, parent), nil
//	}
//	return Normal, nil
//}
//// 删除关键字
//func (bt *Btree) delete(key Key) {
//	_, err := bt.deleteRecursive(key, nil, bt.root)
//	if err != nil {
//		log.Printf("delete failure %s", err)
//	}
//}
//// 递归删除关键字
//func (bt *Btree) deleteRecursive(key Key, parent, cur *BNode) (int, error) {
//	idx := cur.binaryFind(key)
//	if cur.isLeaf {
//		if !compare(cur.nodes[idx].key,"=", key) {
//			return -1, errors.New("this key is not exist")
//		}
//		isUpdate, err := cur.deleteElement(idx)
//		if err != nil {
//			return Normal, err
//		}
//		if isUpdate {
//			// 更新索引节点，把久的索引（本次删除的）换成新的（删除后剩下最大关键字）
//			bt.updateIndex(key, cur.nodes[len(cur.nodes)-1].key, cur.degree)
//		}
//		state := cur.checkBNode(cur == bt.root)
//		if state == Merge {
//			return cur.mergeBNode(bt ,parent), nil
//		}
//		return Normal, nil
//	}
//	state, err := bt.deleteRecursive(key, cur, cur.nodes[idx].childPtr)
//	if err != nil {
//		return Normal, err
//	}
//	if state == Merge {
//		return cur.mergeBNode(bt, parent), nil
//	}
//	return Normal, nil
//}
//// 更新操作
//func (bt *Btree) update(key Key, value interface{}) {
//	// 找到叶子节点的关键字，更新值
//	if bt.findBySqt(key) == nil {
//		panic("the key is not exist")
//	}
//	node := bt.findBySqt(key)
//	node.value = value
//}
//// 在插入操作时，如果插入的新关键字最为最大（最小）关键字，则需要从root节点开始进行更新索引(指定深度degree)
//// 仅修改 key，不改变指针
//func (bt *Btree) updateIndex(oldIndex, newIndex Key, degree int) {
//	cur := bt.root
//	for cur.degree > degree {
//		idx := cur.binaryFind(oldIndex)
//		if compare(cur.nodes[idx].key, "=",oldIndex) {
//			cur.nodes[idx].key = newIndex
//		}
//		cur = cur.nodes[idx].childPtr
//	}
//}
//
//type BNode struct {
//	isLeaf bool
//	m int // 阶数
//	nodes []*SNode
//	next *BNode // 叶子节点指向临近节点的指针
//	degree int // 节点所处的树的高度，叶子节点为0，root最高
//}
//
//func newBNode(isLeaf bool, m int, nodes []*SNode, next *BNode, degree int) *BNode {
//	return &BNode{isLeaf: isLeaf, m: m, nodes: nodes, next: next, degree:degree}
//}
//// 在该节点进行顺序查找（二分查找）
//// 返回找到的SNode的序号
//func (bn *BNode) binaryFind(key Key) (int) {
//	if len(bn.nodes) == 0 {
//		return 0
//	}
//	left := 0
//	right := len(bn.nodes) - 1
//	for left < right {
//		mid := left + (right - left) / 2
//		if compare(bn.nodes[mid].key, "=", key) {
//			return mid
//		}
//		if compare(bn.nodes[mid].key, ">", key) {
//			right = mid
//		}else {
//			left = mid + 1
//		}
//	}
//	return left
//}
//// 递归查找BNode直达叶子节点
//func (bn *BNode) findBNode(key Key) (*BNode, int) {
//	// 边界
//	idx := bn.binaryFind(key)
//	if bn.isLeaf {
//		return bn, idx
//	}
//	// 搜索
//	return bn.nodes[idx].childPtr.findBNode(key)
//}
//// 顺序查找从该叶子节点顺序查找
//func (bn *BNode) findLeafBNode(key Key) (*BNode, int) {
//	if !bn.isLeaf {
//		return nil, -1
//	}
//	for bn != nil {
//		idx := bn.binaryFind(key)
//		if compare(bn.nodes[idx].key, ">=", key) {
//			return bn, idx;
//		}
//		bn = bn.next
//	}
//	if bn == nil {
//		return bn, -1
//	}
//	return bn, len(bn.nodes)-1 // 如果inRecord比已经存在的关键字都大
//}
//// 插入元素
//// return 是否需要更新索引节点
//func (bn *BNode) insertElement(idx int, newSn *SNode) (bool, error) {
//	if bn.nodes == nil {
//		bn.nodes = []*SNode{newSNode(newSn.key, newSn.childPtr, newSn.value)}
//		return false, nil
//	}
//	if compare(newSn.key, "=", bn.nodes[idx].key) {
//		return false, errors.New("key is exist")
//	}
//	if compare(newSn.key, ">", bn.nodes[idx].key){
//		bn.nodes = insertNodes(bn.nodes, idx+1, newSn)
//		return true, nil
//	}
//	bn.nodes = insertNodes(bn.nodes, idx, newSn)
//	return false, nil
//}
//// 删除元素
//// return 是否需要更新索引节点
//func (bn *BNode) deleteElement(idx int) (bool, error) {
//	bLen := len(bn.nodes)
//	if bLen == 0 || idx >= bLen {
//		return false, errors.New("the index is range out")
//	}
//	if idx == bLen - 1 {
//		bn.nodes = bn.nodes[:idx]
//		return true, nil
//
//	}
//	bn.nodes = append(bn.nodes[:idx], bn.nodes[idx+1:]...)
//	return false, nil
//}
//// 查看兄弟节点是否还有多余位置(insert)
//func (bn *BNode) hasFreePos(parent *BNode) (bool, *BNode, int) {
//	if parent == nil {
//		return  false, nil, -1
//	}
//	bkey := bn.nodes[len(bn.nodes)-1].key // 该节点在父节点的索引值（最大关键字）
//	bidx := parent.binaryFind(bkey)       // 在父节点的索引
//	var rightNode, leftNode *BNode
//	if bidx + 1 < len(parent.nodes) { // 右兄弟
//		rightNode = parent.nodes[bidx+1].childPtr
//		if len(rightNode.nodes) < bn.m {
//			return true, rightNode, 0 // 可以插入右兄弟的位置
//		}
//	}
//	if bidx - 1 >= 0 { // 左兄弟
//		leftNode = parent.nodes[bidx-1].childPtr
//		if len(leftNode.nodes) < bn.m {
//			return true, leftNode, len(leftNode.nodes) - 1 // 可以插入右兄弟的位置
//		}
//	}
//	return false, nil, -1 // 没有多余位置
//}
//// 询问兄弟节点是否还有多余关键字(delete)
//func (bn *BNode) hasFreeKey(parent *BNode) (bool, *BNode, int) {
//	if parent == nil {
//		return false, nil, -1
//	}
//	bkey := bn.nodes[len(bn.nodes)-1].key // 该节点在父节点的索引值（最大关键字）
//	bidx := parent.binaryFind(bkey)       // 在父节点的索引
//	var rightNode, leftNode *BNode
//	if bidx + 1 < len(parent.nodes) { // 右兄弟
//		rightNode = parent.nodes[bidx+1].childPtr
//		if len(rightNode.nodes) > (bn.m + 1) / 2 {
//			return true, rightNode, 0 // 第一个节点
//		}
//	}
//	if bidx - 1 >= 0 { // 左兄弟
//		leftNode = parent.nodes[bidx-1].childPtr
//		if len(leftNode.nodes) > (bn.m + 1) / 2 {
//			return true, leftNode, len(leftNode.nodes) - 1 // 最后一个节点
//		}
//	}
//	if rightNode != nil {
//		return false, rightNode, -1 // 左右兄弟都没有多余的key
//	}
//	if leftNode != nil {
//		return false, leftNode, -1
//	}
//	return false, nil, -1 //如果没有兄弟节点，这种情况应该不存在，因为父节点一定时符合要求的，那么一定会有兄弟节点
//}
//// 检查该节点的关键字是否满足要求，不满足则进行对应的操作
//func (bn *BNode) checkBNode(isRoot bool) int {
//	count := len(bn.nodes)
//	upLimit := bn.m
//	lowerLimit := (bn.m + 1) >> 1
//	if isRoot {
//		lowerLimit = 1
//	}
//	if count > upLimit { // 超出限制，分裂
//		return Split
//	}
//	if count < lowerLimit { // 太少了，合并
//		return Merge
//	}
//	return Normal
//}
//// 分裂该节点（分裂之前要更新好索引节点的索引）
//// 如果parent节点也需要分裂就返回 Split 标记
//func (bn *BNode) splitBNode(bt *Btree, parent *BNode) (int) {
//	// 1. 先检查兄弟节点是否有空位置放
//	// 2. 没有就分裂
//	ok, brother, brohterIdx := bn.hasFreePos(parent)
//	if ok {
//		if brohterIdx == 0 { // 右兄弟，给该节点最大的关键字，本节点删除该关键字，更新索引
//			insertNodes(brother.nodes, brohterIdx, bn.nodes[len(bn.nodes) - 1])
//			bn.deleteElement(len(bn.nodes) - 1)
//			bt.updateIndex(brother.nodes[0].key, bn.nodes[len(bn.nodes) - 1].key, bn.degree)
//		} else { // 左兄弟，给该节点最小的关键字，本节点删除该关键字，更新索引
//			brother.nodes = append(brother.nodes, bn.nodes[0])
//			bn.deleteElement(0)
//			bt.updateIndex(brother.nodes[brohterIdx].key, brother.nodes[brohterIdx+1].key, brother.degree)
//		}
//		return Normal
//	}
//	m := bn.m + 1
//	leftNodes := make([]*SNode, 0, m>>1)
//	rightNodes := make([]*SNode, 0, (m + 1)>>1)
//	leftNodes = bn.nodes[:m>>1]
//	rightNodes = bn.nodes[m>>1:]
//	newBn := newBNode(bn.isLeaf, bn.m, rightNodes, bn.next, bn.degree)
//	bn.nodes = leftNodes
//	if bn.isLeaf {
//		bn.next = newBn
//	}
//	var newSnL *SNode
//	if bn.isLeaf {
//		newSnL = newSNode(bn.nodes[len(bn.nodes)-1].key, bn, bn.nodes[len(bn.nodes)-1].value)
//	} else {
//		newSnL = newSNode(bn.nodes[len(bn.nodes)-1].key, bn, nil)
//	}
//	if parent == nil { // 生成新的root节点
//		newSnR := newSNode(newBn.nodes[len(bn.nodes)-1].key, newBn, nil)
//		parent = newBNode(false, bn.m, []*SNode{newSnL, newSnR}, nil, bn.degree+1)
//		bt.root = parent // 更新
//	} else {
//		idx := parent.binaryFind(newSnL.key)
//		for _, node := range parent.nodes { // 修改原parent节点指向bn的指针要指针新创建的节点newBn
//			if node.childPtr == bn {
//				node.childPtr = newBn
//			}
//		}
//		parent.insertElement(idx, newSnL)
//		return parent.checkBNode(parent == bt.root)
//	}
//	return Normal
//}
//// 合并节点（删除操作时）和兄弟节点合并
//func (bn *BNode) mergeBNode(bt *Btree, parent *BNode) (int) {
//	if parent == nil { // 可能和checkBNode有点重复
//		return Normal // 单节点时删除不用检查
//	}
//	// 1.需要判断兄弟节点是否有多余关键字可以分配
//	// 2.如果没有才进行合并
//	ok, brother, brotherIdx := bn.hasFreeKey(parent)
//	if ok {
//		tmp := brother.nodes[brotherIdx]
//		brother.deleteElement(brotherIdx) // 删除
//		if brotherIdx == 0 { // 右兄弟
//			bn.nodes = append(bn.nodes, tmp)
//			bt.updateIndex(bn.nodes[len(bn.nodes)-2].key, tmp.key, bn.degree)
//		} else { // 左兄弟
//			bn.nodes = insertNodes(bn.nodes, 0, tmp)
//			bt.updateIndex(tmp.key, brother.nodes[len(bn.nodes)-1].key, brother.degree)
//		}
//		return Normal
//	}
//	// 合并 brother 和 bn 节点 返回是否需要继续合并(需要注意更新索引节点)
//	var left, right *BNode
//	if compare(bn.nodes[0].key, ">", brother.nodes[0].key) {
//		left = brother
//		right = bn
//	} else {
//		left = bn
//		right = brother
//	}
//	oldIdx := len(left.nodes) - 1
//	parent.deleteElement(parent.binaryFind(left.nodes[oldIdx].key)) // 删除left节点的最大关键字索引
//	left.nodes = append(left.nodes, right.nodes...)
//	right.nodes = left.nodes
//	return parent.checkBNode(parent == bt.root)
//}
//
//type Key interface {
//	// 比较当前的key是否小于参数key
//	// 如果 !a.Less(b) && !b.Less(a) ==> a == b
//	Less(than Key) bool
//}
//
//type SNode struct {
//	key      Key
//	childPtr *BNode
//	value    interface{} // 叶子小节点指向value的指针(或者值)
//}
//
//func newSNode(key Key, childPtr *BNode, value interface{}) *SNode {
//	return &SNode{key: key, childPtr: childPtr, value:value}
//}
//
//// 依照运算符 op 对r1和r2进行比较运算
//func compare(r1 Key, op string, r2 Key) bool {
//	switch op {
//	case ">":
//		if r2.Less(r1) { // r1 > r2
//			return true
//		}
//	case ">=":
//		if !r1.Less(r2) { // r1 >= r2
//			return true
//		}
//	case "<":
//		if r1.Less(r2) { // r1 < r2
//			return true
//		}
//	case "<=":
//		if !r2.Less(r1) { // r1 <= r2
//			return true
//		}
//	case "=":
//		if !r1.Less(r2) && !r2.Less(r1) {
//			return true
//		}
//	}
//	return false
//}
//
//// 向nodes数组中插入某个record(位置为idx)，原idx位置的元素需要比插入元素小
//func insertNodes(nodes []*SNode, idx int, snode *SNode) []*SNode {
//	tmp := append([]*SNode{}, nodes[idx:]...)
//	nodes = append(nodes[:idx], snode)
//	nodes = append(nodes, tmp...)
//	return nodes
//}
