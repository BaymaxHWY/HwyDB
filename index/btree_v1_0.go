package index

// 数据结构需求与设计 v1.0
// 需求：
//		1. 每个internal node需要多个key，每个key需要配left和right两个指针，而internal node需要一个指向父节点的指针
//		2. 每个leaf node需要多个key，每个key需要一个data指针，而leaf node需要一个next指针和父节点指针
// 设计：先不考虑对外提供接口，先实现一个单文件的
//		1. 一个BTnode包含多个小节点node
//		2. 小节点node分为两种一个是internal的小node，一个是leaf的小node
//
//const (
//	M = 5
//)
//
//type BTree struct {
//	root *BTnode // 根节点指针
//	minKey *BTnode // 最小关键字节点指针（暂时不用）
//	degree int // 树阶数
//	btCount int // BTnode节点的个数
//}
//
//func (B *BTree) Search(inRecord Record) interface{} {
//	node, err := B.root.searchNodeForValue(inRecord)
//	if err != nil {
//		log.Printf("%v, %s", inRecord, err)
//		return nil
//	}
//	return node.record.GetValue()
//}
//
//type BTnode struct {
//	isRoot       bool     // 判断是否是根节点
//	isLeaf       bool     // 判断是leaf还是internal
//	degree       int      // 阶数，限制每个BTnode最多有 m-1 个key，最少有 m/2 个key
//	nodes        []*Snode // 暂时leaf、internal使用同一个小node
//	parent, next *BTnode
//	count        int // 当前节点包含小node的个数
//}
//
//func (t *BTnode) Insert(inRecord Record) error {
//	if t == nil { // 说明是空树，创建leaf node
//		t = NewBTnode(M, true, true) // 这里应该从配置文件里读取
//		t.nodes = make([]*Snode, 0, t.degree-1)
//		t.nodes = append(t.nodes, NewSnode(inRecord, nil, nil))
//		t.count++
//		return nil
//	}
//	// 非空树
//	t.insertNodes(inRecord, nil, nil)
//	return nil
//}
//
//func (t *BTnode) checkNode() {
//	upperLimit := t.degree - 1 // key的上限
//	lowerLimit := t.degree / 2 // key的下限
//	if t.isRoot {         // 如果是根节点的话
//		lowerLimit = 1
//	}
//	if t.count > upperLimit {
//		t.splitNode() // 需要分裂
//	} else if t.count < lowerLimit {
//		t.mergeNode() // 需要合并
//	}
//}
//
//func (t *BTnode) splitNode() {
//	// 对于internal node 和 leaf node的分裂操作不同
//	// 对于leaf node
//	// 1. 创建一个新的节点t1（右节点），本节点成为左节点(next指针指向右节点)
//	// 2. 将nodes中的前 M/2 个记录放到左节点，剩下的放到右节点
//	// 3. 将第m/2+1个记录的key进位到父结点中（父结点一定是索引类型结点，只有这里才会创建internal node
//	// 4. 进位到父结点的key左孩子指针向左结点,右孩子指针向右结点
//	// 5. 递归检查父节点(索引节点)
//	// 对于 internal node
//	// 1. 创建一个新的节点t1（右节点），本节点成为左节点
//	// 2. 将nodes中的前(m-1)/2个记录放到左节点，(m-(m-1)/2)-1个记录的放到右节点
//	// 3. 将第m/2个记录的key进位到父结点中
//	// 4. 进位到父结点的key左孩子指针向左结点,右孩子指针向右结点
//	// 5. 递归检查父节点(索引节点)
//	var upnode *Snode
//	var parent *BTnode
//	var leftIdx, rightIdx, upIdx int
//	tr := NewBTnode(t.degree, false, t.isLeaf)
//	if t.isLeaf {
//		leftIdx = t.degree/2
//		rightIdx = t.degree/2
//		upIdx = t.degree/2
//	} else {
//		leftIdx = (t.degree-1)/2
//		rightIdx = (t.degree-1)/2+1
//		upIdx = (t.degree-1)/2
//	}
//	tr.nodes = append(tr.nodes, t.nodes[rightIdx:]...)
//	tr.count = len(tr.nodes)
//	tr.parent = t.parent
//	upnode = t.nodes[upIdx]
//	t.nodes = t.nodes[:leftIdx]
//	t.next = tr
//	t.count = len(t.nodes)
//	parent = t.parent
//	// 插入父节点
//	if(parent == nil) {
//		t.isRoot = false
//		tr.isRoot = false
//		parent = NewBTnode(t.degree, true, false)
//		t.parent = parent
//		tr.parent = parent
//	}
//	parent.insertNodes(upnode.record, t, tr)
//}
//
//func (t *BTnode) mergeNode() {
//
//}
//
//// 这里需要重新写查找
//// 两种查找，一种从根节点查找，一种从最小关键字开始查找
//// 向该BTnode节点的nodes中插入，保持key的顺序
//func (t *BTnode) insertNodes(inRecord Record, left, right *BTnode) {
//	// 非空树
//	for i, node := range t.nodes { // 遍历该节点的所以key
//		if node.record.Compare(inRecord.Key()) == 0 { // 如果该key已经存在则更新其data，结束插入
//			node.record.SetValue(inRecord.GetValue())
//			node.left = left
//			node.right = right
//			return
//		}
//		if node.record.Compare(inRecord.Key()) < 0 { // 找到第一个 key 比 key 大的节点
//			newNode := NewSnode(inRecord, left, right)
//			tmpNodes := append([]*Snode{}, t.nodes[i:]...)
//			t.nodes = append(t.nodes[:i], newNode)
//			t.nodes = append(t.nodes, tmpNodes...)
//			t.count++
//			t.checkNode() // 检查该节点是否需要进行分裂
//			return
//		}
//	}
//	// 如果本节点有空余的就优先插入
//	if t.count < t.degree - 1 {
//		newNode := NewSnode(inRecord, left, right)
//		t.nodes = append(t.nodes, newNode)
//		t.count++
//	}else if t.isLeaf && t.next != nil { // 如果本节点没有空余位置并且是leaf node并且存在next节点，则插入next
//		t.next.Insert(inRecord)
//	} else { // 否则插入尾部，检查分裂
//		newNode := NewSnode(inRecord, left, right)
//		t.nodes = append(t.nodes, newNode)
//		t.count++
//		t.checkNode() // 检查该节点是否需要进行分裂
//	}
//}
//
//// 精确查找到值，找不到就返回error
//func (t *BTnode) searchNodeForValue(inRecord Record) (node *Snode, err error) {
//	if t == nil {
//		err = errors.New("this key is not exist")
//		return nil, err
//	}
//	// 在本BTnode节点内进行node查找
//	node = t.searchInNode(inRecord)
//	res := node.record.Compare(inRecord.Key())
//	if  res > 0 { // inRecord大
//		node, err = node.right.searchNodeForValue(inRecord)
//	}else if res < 0 { // inRecord小
//		node, err = node.left.searchNodeForValue(inRecord)
//	}else { // 相等
//		return node, nil
//	}
//	return node, err
//}
//
//// 精确查找到节点
//func (t *BTnode) searchNodeForBT(inRecord Record) (*BTnode) {
//
//}
//
//// 在本节点中的nodes中查找记录
//func (t *BTnode) searchInNode(inRecord Record) (*Snode) {
//
//}
//
//type Snode struct {
//	record Record
//	left, right *BTnode // internal小节点使用
//}
//
//// 自定义接口
//type Record interface {
//	// 如果 input key > this key return 一个大于0的数
//	// 如果 input key == this key return 0
//	// 如果 input key < this key return 一个小于0的数
//	Compare(key interface{}) int64
//	Key() interface{}
//	GetValue() interface{}
//	SetValue(value interface{})
//}
//
//// 先实现简单版本的
//// 每种操作都有两种：针对leaf node 和 internal node
//// 插入操作流程：
//// 一. 向leaf node中插入
//// 		1. 检查是否为空树
////      2. 检查key是否已经存在
////		3. 插入到leaf node中
////			3.1 如果插入之后没有超过M-1 则插入完成
////			3.1 否则需要进行split，需要先将该leaf node进行split，选择第M/2+1个记录插入到internal node中（转向二）
//// 二. 向internal node中插入
//// 		1. 检查该节点是否存在
//// 		2. 如果该记录存在则更改其lefth和right指针
//// 		3. 插入到该节点中，然后检查是否需要split，如果需要再转向1
//
//func NewBTnode(degree int, isRoot, isLeaf bool) *BTnode {
//	return &BTnode{isRoot: isRoot,
//		isLeaf: isLeaf,
//		degree:      degree,
//	}
//}
//
//func NewSnode(record Record, left, right *BTnode) *Snode {
//	return &Snode{
//		record:record,
//		left:  left,
//		right: right,
//	}
//}
