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

// 重写b+tree，实现磁盘存储功能（持久化）
// TODO:
// 	1. 没有进行缓存优化的，没有设置文件大小限制
//  2. 优化insert，如果当前节点超过限制，则匀一个给兄弟节点（如果兄弟节点有空余的话）,
//	 3. val需要设置最大长度（在输入的时候，或者程序默认最大长度，不然如果更新的val长度比原来的大就会覆盖一部分）

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
	Prev 	 OFFTYPE
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
	t.maxBytes = int64(35 + 32 * BN)
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
	}
	return t, nil
}
// 删除操作
func (t *Tree) delete(key OFFTYPE) error {
	var (
		node *Node
		err error
	)
	if t.root == INVAILD_OFFSET { // B+树是空的
		return NotFindKey
	}
	if node, err = t.seekNode(t.root); err != nil { // 获得root节点
		return err
	}
	for !node.IsLeaf { // 找到叶子结点
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
	if idx := t.findKey(node, key); idx != -1 {
		if err = t.deleteKeyFromNode(node, idx); err != nil {
			return err
		}
	}else {
		// 没有找到
		return fmt.Errorf("the key is not exist, %d", key)
	}
	if len(node.Children) >= BN {
		return nil
	}
	// 需要借or合并
	if err = t.handleAfterDelete(node); err != nil {
		return err
	}
	return nil
}
// 合并叶子节点
// TODO:如何处理废弃的节点
func (t *Tree) mergeNode(left *Node, right *Node) error {
	// 注意对父节点的修改
	var (
		err error
		parent *Node
	)
	deleteKey := left.Children[len(left.Children) - 1].Key
	right.Children = append(left.Children, right.Children...)
	if err = t.flushNode(right); err != nil {
		return err
	}
	if parent, err = t.seekNode(right.Parent); err != nil {
		return err
	}
	idx := t.findKey(parent, deleteKey)
	if err = t.deleteKeyFromNode(parent, idx); err != nil {
		return err
	}
	if len(parent.Children) >= BN {
		return nil
	}
	// 如果parent不符合要求
	// 1. parent是root节点
	if parent.Self == t.root {
		t.root = right.Self
		return t.flushTree(t.root);
	}
	// 2. 处理需要借or合并的情况
	if err = t.handleAfterDelete(parent); err != nil {
		return err
	}
	return nil
	// 对父节点需要删除left_last的关键字索引
}
// 处理需要借or合并的情况
func (t *Tree) handleAfterDelete(node *Node) error {
	// 需要借or合并
	// 1.查看兄弟节点是否还有多余关键字（如果有就借过来结束）
	if ok, err, right_off := t.hasMoreKeys(node.Parent, node); err != nil {
		return err
	} else if !ok {
	// 2.需要合并(左右都没有多余关键字，那么可以随意和一个节点合并)，这里默认和 right 节点合并
		var right *Node
		if right, err = t.seekNode(right_off); err != nil {
			return err
		}
		// 合并node和right叶子节点
		if right.Children[0].Key < node.Children[0].Key { // 看那个在右边
			node, right = right, node
		}
		if err = t.mergeNode(node, right); err != nil {
			return err
		}
	}
	return nil
}
// 向上删除索引
//func (t *Tree) deleteKeyFromNode(node *Node, deleteKey OFFTYPE) error {
//	var (
//		parent *Node
//		err error
//	)
//	if node == nil {
//		return nil
//	}
//	if node.Parent == INVAILD_OFFSET {
//		return nil
//	}
//	if parent, err = t.seekNode(node.Parent); err != nil {
//		return err
//	}
//	if idx := t.findKey(parent, deleteKey); idx != -1 {
//		if idx == len(parent.Children) - 1 {
//			oldKey := parent.Children[idx].Key
//			newKey := parent.Children[idx-1].Key
//			parent.Children = parent.Children[:idx]
//			// 更新索引
//			if err = t.updateIndex(parent, newKey, oldKey); err != nil {
//				return err
//			}
//			// 写入磁盘
//			if err = t.flushNode(parent); err != nil {
//				return err
//			}
//		} else {
//			parent.Children = append(parent.Children[:idx], parent.Children[idx+1:]...)
//		}
//	}
//	return nil
//}
// 查看兄弟节点是否还有多余关键字
// 返回 bool：是否借成功
// 		OFFTYP：如果没有借成功的话返回需要合并节点的地址（优先右节点，如果右节点不存在则返回左节点）
func (t *Tree) hasMoreKeys(parent_off OFFTYPE, node *Node) (bool, error, OFFTYPE) {
	var (
		parent *Node
		err error
		left_off OFFTYPE
		right_off OFFTYPE
	)
	left_off = INVAILD_OFFSET
	right_off = INVAILD_OFFSET
	if parent_off == INVAILD_OFFSET {
		// 如果node是root节点的话就不需要受数量的限制
		return true, nil, INVAILD_OFFSET
	}
	if parent, err = t.seekNode(parent_off); err != nil {
		return false, err, INVAILD_OFFSET
	}
	var idx = -1
	for i, child := range parent.Children {
		if child.Val == node.Self {
			idx = i
			break
		}
	}
	if idx < 0 {
		return false, fmt.Errorf("this node is not exist the parent"), INVAILD_OFFSET
	}
	// 左边的兄弟
	if idx - 1 >= 0 {
		var left *Node
		if left, err = t.seekNode(parent.Children[idx-1].Val); err != nil {
			return false, err, INVAILD_OFFSET
		}
		if len(left.Children) > BN {
			// 借最后一位，需要更新left的最大关键字在parent中的索引
			borrow := left.Children[len(left.Children) - 1]
			left.Children = left.Children[:len(left.Children) - 1]
			node.Children = append([]KV{borrow}, node.Children...)
			if err = t.updateIndex(left, left.Children[len(left.Children)-1].Key, borrow.Key); err != nil {
				return false, err, INVAILD_OFFSET
			}
			if err = t.flushMultiNodes(left, node); err != nil {
				return false, err, INVAILD_OFFSET
			}
			return true, nil, INVAILD_OFFSET
		}
		left_off = left.Self
	}
	// 右边的兄弟
	if idx + 1 < len(parent.Children) {
		var right *Node
		if right, err = t.seekNode(parent.Children[idx+1].Val); err != nil {
			return false, err, INVAILD_OFFSET
		}
		if len(right.Children) > BN {
			// 借第一位，需要更新node的最大关键字在parent中的索引
			borrow := right.Children[0]
			right.Children = right.Children[1:]
			node.Children = append(node.Children, borrow)
			if err =  t.updateIndex(node, borrow.Key, node.Children[len(node.Children) - 2].Key); err != nil {
				return false, err, INVAILD_OFFSET
			}
			if err = t.flushMultiNodes(right, node); err != nil {
				return false, err, INVAILD_OFFSET
			}
			return true, nil, INVAILD_OFFSET
		}
		right_off = right.Self
	}
	if right_off == INVAILD_OFFSET {
		return false, nil, left_off
	}
	return false, nil, right_off
}
// 从叶子结点中删除这个关键字(必要时更新索引)
func (t *Tree) deleteKeyFromNode(node *Node, idx int) error {
	var err error
	if node == nil || node.Children == nil {
		return fmt.Errorf("the node or node.Children is nil")
	}
	if idx >= len(node.Children) {
		return fmt.Errorf("index is out range of children")
	}
	if idx == len(node.Children) - 1 {
		//删除的是最后一个关键字，需要更新上层索引
		oldKey := node.Children[idx].Key
		newKey := node.Children[idx-1].Key
		node.Children = node.Children[:idx]
		// 更新索引
		if err = t.updateIndex(node, newKey, oldKey); err != nil {
			return err
		}
	} else {
		node.Children = append(node.Children[:idx], node.Children[idx+1:]...)
	}
	// 写入磁盘
	if err = t.flushNode(node); err != nil {
		return err
	}
	return nil
}
// 更新操作
func (t *Tree) update(key OFFTYPE, val string) error {
	var (
		node *Node
		err error
	)
	if t.root == INVAILD_OFFSET { // B+树是空的
		return NotFindKey
	}
	if node, err = t.seekNode(t.root); err != nil { // 获得root节点
		return err
	}
	for !node.IsLeaf { // 找到叶子结点
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
	for _, child := range node.Children {
		if child.Key == key { // 找到
			if err = t.flushValue(child.Val, val); err != nil {
				return err
			}
		}
	}
	return nil
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
	if err = t.updateIndex(parent, newKv.Key, parent.Children[idxL].Key); err != nil {
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
	if err := t.updateIndex(node, kv.Key, oldKey); err != nil {
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
	if err := t.updateIndex(node, kv.Key, oldKey); err != nil {
		return -1, err
	}
	return idx, nil
}
// 从node开始向上递归更新索引
func (t *Tree) updateIndex(node *Node, newKey, oldKey OFFTYPE) error {
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
		return t.updateIndex(parent, newKey, oldKey)
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
	right.Prev = left.Self
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
	node.Prev = INVAILD_OFFSET
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
	// Prev
	if err := binary.Read(bs, binary.LittleEndian, &node.Prev); err != nil {
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
// 刷新DB文件的对应val
func (t *Tree) flushValue(offset OFFTYPE, new_val string) error {
	var (
		err error
	)
	if t.dbFile == nil {
		if t.dbFile, err = os.OpenFile(DBFile, os.O_CREATE | os.O_RDWR, 0644); err != nil {
			return err
		}
	}
	// 获取val的字节数并序列化
	valBuf := []byte(new_val)
	vlen := uint8(len(valBuf))
	bs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(bs, binary.LittleEndian, vlen); err != nil {
		return err
	}
	// 把val序列化
	if err = binary.Write(bs, binary.LittleEndian, valBuf); err != nil {
		return err
	}
	// 把序列化的结果存储进文件
	if _, err = t.dbFile.WriteAt(bs.Bytes(), int64(offset)); err != nil {
		return err
	}
	return nil
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
	vlen := uint8(len(valBuf))
	bs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(bs, binary.LittleEndian, vlen); err != nil {
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
	// Prev
	if err = binary.Write(bs, binary.LittleEndian, node.Prev); err != nil {
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