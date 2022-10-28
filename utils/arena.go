package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// in this memory pool, offset's type is uint32,
	//used to find where an object start in the memory pool
	offsetSize = int(unsafe.Sizeof(uint32(0)))

	//we need this to help us make node object memory align
	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1
	maxNodeSize = int(unsafe.Sizeof(node{}))
)

type Arena struct {
	//buf' offset, where the next object start
	n          uint32
	shouldGrow bool
	buf        []byte
}

func newArena(n int64) *Arena {
	arena := &Arena{
		n:   1,
		buf: make([]byte, n),
	}

	return arena
}

func (s *Arena) allocate(size uint32) uint32 {
	offset := atomic.AddUint32(&s.n, size)
	if !s.shouldGrow {
		AssertTrue(int(offsetSize) <= len(s.buf))
		return offset - size
	}

	if int(offset) > len(s.buf) {
		grow := uint32(len(s.buf))
		if grow > 1<<30 {
			grow = 1 << 30
		}
		if grow < size {
			grow = size
		}

		newBuf := make([]byte, len(s.buf)+int(grow))
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))

		s.buf = newBuf
	}

	return offset - size
}

func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) putNode(height int) uint32 {
	unUsedSize := (maxHeight - height) * offsetSize

	l := uint32(maxNodeSize - unUsedSize + nodeAlign)

	//because key and value are not memory align,
	//we should alloc more memory to make the alloced memory align
	allocOffset := s.allocate(l)

	//after the padding (the part make the alloced memory align), the remaining area is where the node data save
	nodeOffset := (allocOffset + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return nodeOffset
}

func (s *Arena) putKey(key []byte) uint32 {
	keySize := uint32(len(key))
	offset := s.allocate(keySize)
	buf := s.buf[offset : offset+keySize]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (s *Arena) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := s.allocate(l)
	v.EncodeValue(s.buf[offset:])
	return offset
}

func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}

func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
