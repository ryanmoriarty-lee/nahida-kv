package utils

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"
)

const (
	maxHeight = 20

	//use to controls the probability of generating height
	heightIncrease = math.MaxUint32 / 2
)

// linklist node
type node struct {
	//   value offset: uint32 (bits 0-31)
	//   value size  : uint16 (bits 32-63)
	value uint64

	keyOffset uint32
	keySize   uint16

	height uint16
	tower  [maxHeight]uint32
}

type Skiplist struct {
	height     int32
	headOffset uint32
	ref        int32
	arena      *Arena
	OnClose    func()
}

func (s *Skiplist) IncrRef() {
	atomic.AddInt32(&s.ref, 1)
}

func (s *Skiplist) DecrRef() {
	newRef := atomic.AddInt32(&s.ref, -1)
	if newRef > 0 {
		return
	}

	if s.OnClose != nil {
		s.OnClose()
	}

	s.arena = nil
}

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	nodeOffset := arena.putNode(height)
	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	node := arena.getNode(nodeOffset)
	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.height = uint16(height)
	node.value = val

	return node
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

func NewSkiplist(arenaSize int64) *Skiplist {
	arena := newArena(arenaSize)
	head := newNode(arena, nil, ValueStruct{}, maxHeight)
	headOffset := arena.getNodeOffset(head)
	return &Skiplist{
		height:     1,
		headOffset: headOffset,
		ref:        1,
		arena:      arena,
	}
}

func (n *node) getValueOffset() (uint32, uint32) {
	value := n.value
	return decodeValue(value)
}

func (n *node) key(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

func (n *node) setValue(arena *Arena, value uint64) {
	atomic.StoreUint64(&n.value, value)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.tower[h])
}

func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.tower[h], old, val)
}

func (n *node) getVs(arena *Arena) ValueStruct {
	valOffset, valSize := decodeValue(n.value)
	return arena.getVal(valOffset, valSize)
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand() <= heightIncrease {
		h++
	}

	return h
}

func (s *Skiplist) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *Skiplist) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func (s *Skiplist) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

func (s *Skiplist) findNear(key []byte, less bool, allowEqual bool) (*node, bool) {
	x := s.getHead()
	level := int(s.getHeight())

	for {
		next := s.getNext(x, level)
		if next == nil {
			if level > 0 {
				level--
				continue
			}

			if !less {
				return nil, false
			}

			if x == s.getHead() {
				return nil, false
			}

			return x, false
		}

		nextKey := next.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp > 0 {
			x = next
			continue
		}

		if cmp == 0 {
			if allowEqual {
				return next, true
			}

			if !less {
				return s.getNext(next, 0), false
			}

			if level > 0 {
				level--
				continue
			}

			if x == s.getHead() {
				return nil, false
			}
			return x, false
		}

		if level > 0 {
			level--
			continue
		}

		if !less {
			return next, false
		}

		if x == s.getHead() {
			return nil, false
		}

		return x, false
	}
}

func (s *Skiplist) findSpliceForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	for {
		beforeNode := s.arena.getNode(before)
		next := beforeNode.getNextOffset(level)
		nextNode := s.arena.getNode(next)
		if nextNode == nil {
			return before, next
		}

		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		if cmp == 0 {
			return next, next
		}

		if cmp < 0 {
			return before, next
		}

		before = next
	}
}

func (s *Skiplist) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	listHeight := s.getHeight()
	var prev [maxHeight + 1]uint32
	var next [maxHeight + 1]uint32

	prev[listHeight] = s.headOffset
	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = s.findSpliceForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			valueOffset := s.arena.putVal(v)
			encValue := encodeValue(valueOffset, v.EncodedSize())
			prevNode := s.arena.getNode(prev[i])
			prevNode.setValue(s.arena, encValue)
			return
		}
	}

	height := s.randomHeight()
	x := newNode(s.arena, key, v, height)

	listHeight = s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	for i := 0; i < height; i++ {
		for {
			if s.arena.getNode(prev[i]) == nil {
				AssertTrue(i > 1)
				prev[i], next[i] = s.findSpliceForLevel(key, s.headOffset, i)
				AssertTrue(prev[i] != next[i])
			}

			x.tower[i] = next[i]
			pnode := s.arena.getNode(prev[i])
			if pnode.casNextOffset(i, next[i], s.arena.getNodeOffset(x)) {
				break
			}

			prev[i], next[i] = s.findSpliceForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				valueOffet := s.arena.putVal(v)
				encValue := encodeValue(valueOffet, v.EncodedSize())
				preNode := s.arena.getNode(prev[i])
				preNode.setValue(s.arena, encValue)
				return
			}
		}
	}
}

func (s *Skiplist) Empty() bool {
	return s.findLast() == nil
}

func (s *Skiplist) findLast() *node {
	n := s.getHead()
	level := int(s.getHeight()) - 1
	for {
		next := s.getNext(n, level)
		if next != nil {
			n = next
			continue
		}

		if level == 0 {
			if n == s.getHead() {
				return nil
			}
			return n
		}
		level--
	}
}

func (s *Skiplist) Search(key []byte) ValueStruct {
	n, _ := s.findNear(key, false, true)
	if n == nil {
		return ValueStruct{}
	}

	nextKey := s.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}

	valOffset, valSize := n.getValueOffset()
	val := s.arena.getVal(valOffset, valSize)

	return val
}

// NewIterator returns a skiplist iterator.  You have to Close() the iterator.
func (s *Skiplist) NewSkipListIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

// MemSize returns the size of the Skiplist in terms of how much memory is used within its internal
// arena.
func (s *Skiplist) MemSize() int64 { return s.arena.size() }

// Draw plot Skiplist, align represents align the same node in different level
func (s *Skiplist) Draw(align bool) {
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			next = s.getNext(next, level)
			if next != nil {
				key := next.key(s.arena)
				vs := next.getVs(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// align
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// plot
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

// Iterator is an iterator over skiplist object. For new objects, you just
// need to initialize Iterator.list.
type SkipListIterator struct {
	list *Skiplist
	n    *node
}

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

// Close frees the resources held by the iterator
func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

// Valid returns true iff the iterator is positioned at a valid node.
func (s *SkipListIterator) Valid() bool { return s.n != nil }

// Key returns the key at the current position.
func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns value.
func (s *SkipListIterator) Value() ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// ValueUint64 returns the uint64 value of the current node.
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.value
}

// Next advances to the next position.
func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

// Prev advances to the previous position.
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	s.n, _ = s.list.findNear(s.Key(), true, false) // find <. No equality allowed.
}

// Seek advances to the first entry with a key >= target.
func (s *SkipListIterator) Seek(target []byte) {
	s.n, _ = s.list.findNear(target, false, true) // find >=.
}

// SeekForPrev finds an entry with key <= target.
func (s *SkipListIterator) SeekForPrev(target []byte) {
	s.n, _ = s.list.findNear(target, true, true) // find <=.
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

// UniIterator is a unidirectional memtable iterator. It is a thin wrapper around
// Iterator. We like to keep Iterator as before, because it is more powerful and
// we might support bidirectional iterators in the future.
type UniIterator struct {
	iter     *Iterator
	reversed bool
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
