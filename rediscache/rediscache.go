package rediscache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
)

/* Errors */
var ErrInvalidInteger = errors.New("Invalid character found in integer field")
var ErrUnknownCommand = errors.New("Unknown command")

/*A couple of util functions so we don't add allocs*/
const _ALPHA_DIFF byte = 'a' - 'A'

func Upper(b []byte) []byte {
	// Simple in place upper case function
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] >= 'a' && b[i] <= 'z' {
			b[i] -= _ALPHA_DIFF
		}
	}
	return b
}

func ByteToInt(b byte) (int, error) {
	// Convert a single byte to an int
	if b >= '0' && b <= '9' {
		return int(b - '0'), nil
	} else {
		return 0, ErrInvalidInteger
	}
}

var BYTES_GET []byte = []byte("GET")
var BYTES_SET []byte = []byte("SET")
var BYTES_SHUTDOWN []byte = []byte("SHUTDOWN")
var BYTES_NOW []byte = []byte("NOW")
var BYTES_OK []byte = []byte("+OK\r\n")
var BYTES_CRLF []byte = []byte("\r\n")
var BYTES_NULL []byte = []byte("$-1\r\n")
var BYTES_ERR []byte = []byte("-ERR ")

/* Cache interface that the handler uses to communicate with backend */
type Cache interface {
	Get(key []byte, value []byte) []byte
	Set(key []byte, value []byte)
	Stop()
}

/* Commands */
const (
	NONE = iota
	GET
	SET
	ERR
)

/* Redis protocol (RESP) states */
const (
	RESP_START            = iota //  0
	RESP_DONE                    //  1
	RESP_ARG_COUNT               //  2
	RESP_CMD_LENGTH_START        //  3
	RESP_CMD_LENGTH              //  4
	RESP_CMD                     //  5
	RESP_TERM                    //  6
	RESP_ERR                     //  7
	RESP_HANDLE                  //  8
	/* SET */
	RESP_SET_KEY_LENGTH_START   //  9
	RESP_SET_KEY_LENGTH         // 10
	RESP_SET_KEY                // 11
	RESP_SET_VALUE_LENGTH_START // 12
	RESP_SET_VALUE_LENGTH       // 13
	RESP_SET_VALUE              // 14
	/* GET */
	RESP_GET_KEY_LENGTH_START // 15
	RESP_GET_KEY_LENGTH       // 16
	RESP_GET_KEY              // 17
)

/* RESP Handler struct and functions */
type RESPhandler struct {
	cache  Cache
	reader *bufio.Reader
	writer *bufio.Writer
	state  int    // Our current state
	cmd    int    // Parsed command
	key    []byte // Key buffer
	value  []byte // Value buffer
	data   []byte // Data buffer
	cmdbuf []byte // Command buffer
	err    error  // Error
}

func NewRESPhandler(cache Cache) *RESPhandler {
	return &RESPhandler{
		key:    make([]byte, 4096),
		value:  make([]byte, 4096),
		cmdbuf: make([]byte, 0),
		data:   make([]byte, 4096),
		cache:  cache,
	}
}

func (p *RESPhandler) Reset(reader *bufio.Reader, writer *bufio.Writer) {
	// Reset the reader and writer
	p.reader = reader
	p.writer = writer
	p.reset()
}

func (p *RESPhandler) reset() {
	// Reset the internal state
	p.cmd = NONE
	p.state = RESP_START
	p.key = p.key[:0]
	p.value = p.value[:0]
	p.cmdbuf = p.cmdbuf[:0]
	p.err = nil
}

func (p *RESPhandler) Parse() error {
	// TODO: Handle error cases
	pos := 0
	count := 0
	length := 0
	datalen := 0
	val := 0
	p.reset()
	for p.state != RESP_DONE {
		// Read in more data if we need to
		for p.state != RESP_HANDLE && pos >= datalen {
			// TODO: Need better handling of networking edge cases
			pos = pos - datalen
			datalen, p.err = p.reader.Read(p.data)
			/*
				if datalen > 0 {
					fmt.Println(datalen, string(p.data))
				}
			*/
			if p.err != nil {
				return p.err
			}
		}
		switch p.state {
		case RESP_START:
			switch p.data[pos] {
			case '*':
				p.state = RESP_ARG_COUNT
				count = 0
			}
		case RESP_ARG_COUNT:
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_CMD_LENGTH_START
				if p.data[pos] == '\r' {
					pos++
				}
			default:
				val, p.err = ByteToInt(p.data[pos])
				if p.err != nil {
					p.state = RESP_ERR
				} else {
					count = length*10 + val
				}
			}
		case RESP_CMD_LENGTH_START:
			switch p.data[pos] {
			case '$':
				p.state = RESP_CMD_LENGTH
				length = 0
			}
		case RESP_CMD_LENGTH:
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_CMD
				if p.data[pos] == '\r' {
					pos++
				}
			default:
				val, p.err = ByteToInt(p.data[pos])
				if p.err != nil {
					p.state = RESP_ERR
				} else {
					length = length*10 + val
				}
			}
		case RESP_CMD:
			if pos+length > datalen {
				p.cmdbuf = append(p.cmdbuf, p.data[pos:]...)
				length -= datalen - pos
				pos = datalen - 1
			} else {
				if pos > cap(p.data) || pos+length > cap(p.data) {
					fmt.Printf("debugCheckA %d %d %d %d %#v\n", pos, length, cap(p.data), datalen, string(p.data))
				}
				if pos < 0 || length < 0 {
					fmt.Printf("debugCheckB %d %d %d %d %#v\n", pos, length, cap(p.data), datalen, string(p.data))
				}
				p.cmdbuf = Upper(append(p.cmdbuf, p.data[pos:pos+length]...))
				pos += length
				count--
				if bytes.Equal(p.cmdbuf, BYTES_SET) {
					p.cmd = SET
					p.state = RESP_SET_KEY_LENGTH_START
				} else if bytes.Equal(p.cmdbuf, BYTES_GET) {
					p.cmd = GET
					p.state = RESP_GET_KEY_LENGTH_START
				}
			}
		case RESP_TERM:
			// Handles parsing the final "\r\n" of the current cmdbuf
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_HANDLE
				if p.data[pos] == '\r' {
					pos++
				}
			}
		case RESP_ERR:
			p.cmd = ERR
			p.state = RESP_HANDLE
		case RESP_HANDLE:
			switch p.cmd {
			case SET:
				p.cache.Set(p.key, p.value)
				p.writer.Write(BYTES_OK)
			case GET:
				p.value = p.cache.Get(p.key, p.value)
				if p.value != nil {
					p.writer.WriteByte('$')
					p.writer.WriteString(strconv.Itoa(len(p.value)))
					p.writer.Write(BYTES_CRLF)
					p.writer.Write(p.value)
					p.writer.Write(BYTES_CRLF)
				} else {
					p.writer.Write(BYTES_NULL)
				}
			case ERR:
				p.writer.Write(BYTES_ERR)
				p.writer.WriteString(p.err.Error())
				p.writer.Write(BYTES_CRLF)
				p.writer.Flush()
				fmt.Println(p.err.Error())
				// Hangup after an error
				return p.err
			default:
				errstr := fmt.Sprintf("unknown command %#v", p.cmd)
				p.writer.Write(BYTES_ERR)
				p.writer.WriteString(errstr)
				p.writer.Write(BYTES_CRLF)
				p.writer.Flush()
				fmt.Println(errstr)
				// Hangup after the error
				return ErrUnknownCommand
			}
			if pos == datalen {
				p.state = RESP_DONE
				p.writer.Flush()
			} else {
				p.reset()
				pos--
			}

		/* SET states */
		case RESP_SET_KEY_LENGTH_START:
			switch p.data[pos] {
			case '$':
				p.state = RESP_SET_KEY_LENGTH
				length = 0
			}
		case RESP_SET_KEY_LENGTH:
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_SET_KEY
				if p.data[pos] == '\r' {
					pos++
				}
			default:
				val, p.err = ByteToInt(p.data[pos])
				if p.err != nil {
					p.state = RESP_ERR
				} else {
					length = length*10 + val
				}
			}
		case RESP_SET_KEY:
			if pos+length > datalen {
				p.key = append(p.key, p.data[pos:]...)
				length -= datalen - pos
				pos = datalen - 1
			} else {
				p.key = append(p.key, p.data[pos:pos+length]...)
				pos += length
				count--
				p.state = RESP_SET_VALUE_LENGTH_START
			}
		case RESP_SET_VALUE_LENGTH_START:
			switch p.data[pos] {
			case '$':
				p.state = RESP_SET_VALUE_LENGTH
				length = 0
			}
		case RESP_SET_VALUE_LENGTH:
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_SET_VALUE
				if p.data[pos] == '\r' {
					pos++
				}
			default:
				val, p.err = ByteToInt(p.data[pos])
				if p.err != nil {
					p.state = RESP_ERR
				} else {
					length = length*10 + val
				}
			}
		case RESP_SET_VALUE:
			if pos+length > datalen {
				p.value = append(p.value, p.data[pos:]...)
				length -= datalen - pos
				pos = datalen - 1
			} else {
				if pos > cap(p.data) || pos+length > cap(p.data) {
					fmt.Printf("debugCheckC %d %d %d %d %#v\n", pos, length, cap(p.data), datalen, string(p.data))
				}
				if pos < 0 || length < 0 {
					fmt.Printf("debugCheckD %d %d %d %d %#v\n", pos, length, cap(p.data), datalen, string(p.data))
				}
				p.value = append(p.value, p.data[pos:pos+length]...)
				pos += length
				count--
				// TODO: Hadle extra set args
				p.state = RESP_TERM
			}

		/* GET states */
		case RESP_GET_KEY_LENGTH_START:
			switch p.data[pos] {
			case '$':
				p.state = RESP_GET_KEY_LENGTH
				length = 0
			}
		case RESP_GET_KEY_LENGTH:
			switch p.data[pos] {
			case '\r', '\n':
				p.state = RESP_GET_KEY
				if p.data[pos] == '\r' {
					pos++
				}
			default:
				val, p.err = ByteToInt(p.data[pos])
				if p.err != nil {
					p.state = RESP_ERR
				} else {
					length = length*10 + val
				}
			}
		case RESP_GET_KEY:
			if pos+length > datalen {
				p.key = append(p.key, p.data[pos:]...)
				length -= datalen - pos
				pos = datalen - 1
			} else {
				p.key = append(p.key, p.data[pos:pos+length]...)
				pos += length
				count--
				p.state = RESP_TERM
			}
		}
		pos++
	}
	return nil
}
