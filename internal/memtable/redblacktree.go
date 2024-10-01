package memtable

/*
Our memtable will use a Red-Black tree as its under-the-hood implementation
Meant to replace our original hash-table
*/

type color int

const (
	red color = iota
	black
)

type Node struct {
	Key    string
	Value  string
	Parent *Node
	Left   *Node
	Right  *Node
	Color  color
}

type RedBlackTree struct {
	root *Node
}

func NewRedBlackTree() *RedBlackTree {
	// impl
}

func (rbt *RedBlackTree) Insert(key, value string) {
	// impl
}

func (rbt *RedBlackTree) Find(key string) {
	// impl
}

func (rbt *RedBlackTree) rotateLeft(node *Node) {
	// impl
}

func (rbt *RedBlackTree) rotateRight(node *Node) {
	// impl
}
