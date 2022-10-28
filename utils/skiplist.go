package utils

import (
	"math"
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
	value int64

	keyOffset uint32
	keySize   uint16

	height uint16
	tower  [maxHeight]uint32
}
