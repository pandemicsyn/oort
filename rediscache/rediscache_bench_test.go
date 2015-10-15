package rediscache

import (
	"bufio"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/gholt/ring"
)

type NoCache struct{}

func (c NoCache) Get(key []byte, value []byte) []byte {
	return nil
}
func (c NoCache) Set(key []byte, value []byte) {}

func (c NoCache) Start()               {}
func (c NoCache) Stop()                {}
func (c NoCache) Stats() []byte        { return []byte("") }
func (c NoCache) UpdateRing(ring.Ring) {}

func Benchmark_ByteToInt(b *testing.B) {
	for n := 0; n < b.N; n++ {
		ByteToInt('7')
	}
}

func Benchmark_ParseSET(b *testing.B) {
	cache := NoCache{}
	p := NewRESPhandler(cache)
	in := "*3\r\n$3\r\nset\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n"
	reader := strings.NewReader(in)
	buf := bufio.NewReader(reader)
	p.Reset(buf, bufio.NewWriter(ioutil.Discard))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Parse()
		reader.Seek(0, 0)
	}
}

func Benchmark_ParseSET4K(b *testing.B) {
	cache := NoCache{}
	p := NewRESPhandler(cache)
	key := strings.Repeat("K", 4096)
	value := strings.Repeat("V", 4096)
	msg := "*3\r\n$3\r\nset\r\n$4096\r\n" + key + "\r\n$4096\r\n" + value + "\r\n"
	reader := strings.NewReader(msg)
	buf := bufio.NewReader(reader)
	p.Reset(buf, bufio.NewWriter(ioutil.Discard))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Parse()
		reader.Seek(0, 0)
	}
}

func Benchmark_ParseGET(b *testing.B) {
	cache := NoCache{}
	p := NewRESPhandler(cache)
	msg := "*2\r\n$3\r\nget\r\n$8\r\ntest_key\r\n"
	reader := strings.NewReader(msg)
	buf := bufio.NewReader(reader)
	p.Reset(buf, bufio.NewWriter(ioutil.Discard))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Parse()
		reader.Seek(0, 0)
	}
}

func Benchmark_ParseGET4K(b *testing.B) {
	cache := NoCache{}
	p := NewRESPhandler(cache)
	key := strings.Repeat("K", 4096)
	msg := "*2\r\n$3\r\nget\r\n$4096\r\n" + key + "\r\n"
	reader := strings.NewReader(msg)
	buf := bufio.NewReader(reader)
	p.Reset(buf, bufio.NewWriter(ioutil.Discard))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		p.Parse()
		reader.Seek(0, 0)
	}
}
