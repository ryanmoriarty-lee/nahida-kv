package utils

import "encoding/binary"

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64

	Version uint64
}

func (vs *ValueStruct) EncodedSize() uint32 {
	size := len(vs.Value) + 1
	encode := sizeVarint(vs.ExpiresAt)
	return uint32(size + encode)
}

func (vs *ValueStruct) EncodeValue(buf []byte) uint32 {
	buf[0] = vs.Meta
	size := binary.PutUvarint(buf[1:], vs.ExpiresAt)
	n := copy(buf[1+size:], vs.Value)
	return uint32(1 + size + n)
}

func (vs *ValueStruct) DecodeValue(buf []byte) {
	vs.Meta = buf[0]
	var size int
	vs.ExpiresAt, size = binary.Uvarint(buf[1:])
	vs.Value = buf[1+size:]
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int //len of header
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}
