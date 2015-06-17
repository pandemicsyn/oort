package valuestore

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
)

const ktBloomFilterHeaderBytes int = 20

// ktBloomFilter is a key+timestamp bloom filter implementation.
type ktBloomFilter struct {
	n       uint64
	p       float64
	salt    uint32
	m       uint32
	kDiv4   uint32
	bits    []byte
	scratch []byte
}

func newKTBloomFilter(n uint64, p float64, salt uint16) *ktBloomFilter {
	m := -((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2))
	return &ktBloomFilter{
		n:       n,
		p:       p,
		salt:    uint32(salt) << 16,
		m:       uint32(math.Ceil(m/8)) * 8,
		kDiv4:   uint32(math.Ceil(m / float64(n) * math.Log(2) / 4)),
		bits:    make([]byte, uint32(math.Ceil(m/8))),
		scratch: make([]byte, 28),
	}
}

func newKTBloomFilterFromMsg(prm *pullReplicationMsg, headerOffset int) *ktBloomFilter {
	n := binary.BigEndian.Uint64(prm.header[headerOffset:])
	p := math.Float64frombits(binary.BigEndian.Uint64(prm.header[headerOffset+8:]))
	salt := binary.BigEndian.Uint16(prm.header[headerOffset+16:])
	m := -((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2))
	return &ktBloomFilter{
		n:       n,
		p:       p,
		salt:    uint32(salt) << 16,
		m:       uint32(math.Ceil(m/8)) * 8,
		kDiv4:   uint32(math.Ceil(m / float64(n) * math.Log(2) / 4)),
		bits:    prm.body,
		scratch: make([]byte, 28),
	}
}

func (ktbf *ktBloomFilter) toMsg(prm *pullReplicationMsg, headerOffset int) {
	binary.BigEndian.PutUint64(prm.header[headerOffset:], ktbf.n)
	binary.BigEndian.PutUint64(prm.header[headerOffset+8:], math.Float64bits(ktbf.p))
	binary.BigEndian.PutUint16(prm.header[headerOffset+16:], uint16(ktbf.salt>>16))
	copy(prm.body, ktbf.bits)
}

func (ktbf *ktBloomFilter) String() string {
	return fmt.Sprintf("ktBloomFilter %p n=%d p=%f salt=%d m=%d k=%d bytes=%d", ktbf, ktbf.n, ktbf.p, ktbf.salt>>16, ktbf.m, ktbf.kDiv4*4, len(ktbf.bits))
}

func (ktbf *ktBloomFilter) add(keyA uint64, keyB uint64, timestamp uint64) {
	// TODO: There are optimization opportunities here as keyA and keyB can be
	// considered to already have good bit distribution and using a hashing
	// function to mix-in timestamp, salt, and i instead of redoing the whole
	// hash each time would be good to test and benchmark.
	scratch := ktbf.scratch
	binary.BigEndian.PutUint64(scratch[4:], keyA)
	binary.BigEndian.PutUint64(scratch[12:], keyB)
	binary.BigEndian.PutUint64(scratch[20:], timestamp)
	for i := ktbf.kDiv4; i > 0; i-- {
		binary.BigEndian.PutUint32(scratch, ktbf.salt|i)
		h1, h2 := murmur3.Sum128(scratch)
		bit := uint32(h1>>32) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h1&0xffffffff) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h2>>32) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
		bit = uint32(h2&0xffffffff) % ktbf.m
		ktbf.bits[bit/8] |= 1 << (bit % 8)
	}
}

func (ktbf *ktBloomFilter) mayHave(keyA uint64, keyB uint64, timestamp uint64) bool {
	scratch := ktbf.scratch
	binary.BigEndian.PutUint64(scratch[4:], keyA)
	binary.BigEndian.PutUint64(scratch[12:], keyB)
	binary.BigEndian.PutUint64(scratch[20:], timestamp)
	for i := ktbf.kDiv4; i > 0; i-- {
		binary.BigEndian.PutUint32(scratch, ktbf.salt|i)
		h1, h2 := murmur3.Sum128(scratch)
		bit := uint32(h1>>32) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h1&0xffffffff) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h2>>32) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		bit = uint32(h2&0xffffffff) % ktbf.m
		if ktbf.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

func (ktbf *ktBloomFilter) reset(salt uint16) {
	b := ktbf.bits
	l := len(b)
	for i := 0; i < l; i++ {
		b[i] = 0
	}
	ktbf.salt = uint32(salt) << 16
}
