package rediscache

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/pandemicsyn/oort/mapstore"
)

func TEst_ByteToInt(t *testing.T) {
	i, err := ByteToInt('0')
	if i != 0 {
		t.Error("0 didn't convert correctly to int")
	}
	if err != nil {
		t.Error("Converting 0 to int caused an error")
	}
	i, err = ByteToInt('9')
	if i != 9 {
		t.Error("9 didn't convert correctly to int")
	}
	if err != nil {
		t.Error("Converting 9 to int caused an error")
	}
	i, err = ByteToInt('a')
	if err == nil {
		t.Error("Converting a to int didn't cause an error")
	}
	if i != 0 {
		t.Error("Should return 0 on error")
	}
}

func Test_Parse_SET1(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	in := "*3\r\n$3\r\nset\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	v := make([]byte, 0)
	v = cache.Get([]byte("test_key"), v)
	if bytes.Compare(v, []byte("test_value")) != 0 {
		fmt.Println("val:", string(v))
		t.Error("Value in cache was not set correctly")
	}
}

func Test_Parse_SET_Errors(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	in := "*a\r\n$3\r\nset\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	err := p.Parse()
	if err != ErrInvalidInteger {
		t.Error("Error not returned for bad integer")
	}
	msg, _ := out.ReadBytes('\n')
	if bytes.Compare(msg[:4], []byte("-ERR")) != 0 {
		t.Error("Error not sent through writer for bad integer")
	}
}

func Test_Parse_SET_pipeline1(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	in := "*3\r\n$3\r\nSET\r\n$8\r\ntest_key\r\n$10\r\ntest_value\r\n*3\r\n$3\r\nSET\r\n$9\r\ntest_key2\r\n$11\r\ntest_value2\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	v := make([]byte, 0)
	v = cache.Get([]byte("test_key"), v)
	if bytes.Compare(v, []byte("test_value")) != 0 {
		t.Error("Value in cache was not set correctly")
	}
	v = make([]byte, 0)
	v = cache.Get([]byte("test_key2"), v)
	if bytes.Compare(v, []byte("test_value2")) != 0 {
		fmt.Println("val:", string(v))
		t.Error("Value2 in cache was not set correctly")
	}
}

func Test_Parse_SET_pipeline2(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	in := "*3\r\n$3\r\nSET\r\n$16\r\nkey:100000050908\r\n$2\r\nxx\r\n*3\r\n$3\r\nSET\r\n$16\r\nkey:200000988183\r\n$3\r\nyyy\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	v := make([]byte, 0)
	v = cache.Get([]byte("key:100000050908"), v)
	if bytes.Compare(v, []byte("xx")) != 0 {
		t.Error("Value in cache was not set correctly")
		fmt.Println("val1:", string(v))
	}
	v = v[:0]
	v = cache.Get([]byte("key:200000988183"), v)
	if bytes.Compare(v, []byte("yyy")) != 0 {
		t.Error("Value2 in cache was not set correctly")
		fmt.Println("val2:", string(v))
	}
}

func Test_Parse_SET_Large(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	key := strings.Repeat("K", 4096)
	value := strings.Repeat("V", 4096)
	in := "*3\r\n$3\r\nset\r\n$4096\r\n" + key + "\r\n$4096\r\n" + value + "\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	v := make([]byte, 0)
	v = cache.Get([]byte(key), v)
	if bytes.Compare(v, []byte(value)) != 0 {
		t.Error("Value in cache was not set correctly")
		fmt.Println("VAL:", len(v))
	}
}

func Test_Parse_SET_Boundaries(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	key := strings.Repeat("K", 2048)
	value := strings.Repeat("V", 2021)
	in := "*3\r\n$3\r\nset\r\n$2048\r\n" + key + "\r\n$2021\r\n" + value + "\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	v := make([]byte, 0)
	v = cache.Get([]byte(key), v)
	if bytes.Compare(v, []byte(value)) != 0 {
		t.Error("Value in cache was not set correctly")
		fmt.Println("VAL:", len(v))
	}
}

func Test_Parse_GET(t *testing.T) {
	cache := mapstore.NewMapCache()
	cache.Set([]byte("test_key"), []byte("test_value"))
	p := NewRESPhandler(cache)
	in := "*2\r\n$3\r\nget\r\n$8\r\ntest_key\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	length, _ := out.ReadBytes('\n')
	if bytes.Compare(length, []byte("$10\r\n")) != 0 {
		t.Error("Value length was not returned correctly")
	}
	value, _ := out.ReadBytes('\n')
	if bytes.Compare(value, []byte("test_value\r\n")) != 0 {
		t.Error("Value was not returned correctly")
	}
}

func Test_Parse_GET_Large(t *testing.T) {
	cache := mapstore.NewMapCache()
	p := NewRESPhandler(cache)
	key := strings.Repeat("K", 4096)
	cache.Set([]byte(key), []byte("test_value"))
	in := "*2\r\n$3\r\nget\r\n$4096\r\n" + key + "\r\n"
	out := new(bytes.Buffer)
	p.Reset(bufio.NewReader(strings.NewReader(in)), bufio.NewWriter(out))
	p.Parse()
	length, _ := out.ReadBytes('\n')
	if bytes.Compare(length, []byte("$10\r\n")) != 0 {
		t.Error("Value length was not returned correctly")
	}
	value, _ := out.ReadBytes('\n')
	if bytes.Compare(value, []byte("test_value\r\n")) != 0 {
		t.Error("Value was not returned correctly")
	}
}
