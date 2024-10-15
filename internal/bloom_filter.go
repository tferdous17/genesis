package internal

import (
	"bitcask-go/utils"
	"hash/fnv"
	"math"
	"os"
)

type BloomFilter struct {
	file         *os.File
	bitArraySize uint32
	bits         []byte
}

const FalsePositiveProbability = 0.01
const NumHashes = 2

func NewBloomFilter(bloomFile *os.File) *BloomFilter {
	return &BloomFilter{file: bloomFile}
}

func (bf *BloomFilter) InitBloomFilterAttrs(numElements uint32) {
	bf.calculateBitArraySize(numElements)
	bf.initBitArray()
}

func (bf *BloomFilter) calculateBitArraySize(numElements uint32) {
	numerator := float64(numElements) * math.Log(FalsePositiveProbability)
	utils.LogCYAN("numerator %d", numerator)
	denominator := math.Ln2 * math.Ln2
	utils.LogCYAN("denominator %d", denominator)
	res := -(numerator / denominator)
	bf.bitArraySize = uint32(math.Ceil(res))
}

func (bf *BloomFilter) initBitArray() {
	bf.bits = make([]byte, bf.bitArraySize)
}

func (bf *BloomFilter) Add(key string) {
	// hash the key n times, and store it into the bits array
	for i := 0; i < NumHashes; i++ {
		hash := fnv.New64()
		hash.Write([]byte(key))
		index := hash.Sum64() % uint64(bf.bitArraySize)
		bf.bits[index/64] |= 1 << (index % 64)
	}
	//fmt.Println(bf.bits)
}

func (bf *BloomFilter) MightContain(key string) bool {
	// ! Bloom filter is probabilistic, so there's a chance to get false positives
	for i := 0; i < NumHashes; i++ {
		hash := fnv.New64()
		hash.Write([]byte(key))
		index := hash.Sum64() % uint64(bf.bitArraySize)
		// If any of the bits are 0, it guarantees this key is NOT in our dataset
		if (bf.bits[index/64] & 1 << (index % 64)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Debug() {
	utils.LogCYAN("Bit arr size: %d", bf.bitArraySize)
	utils.LogCYAN("Bit arr: %v", bf.bits)
}
