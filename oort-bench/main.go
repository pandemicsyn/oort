package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Scrambled struct {
	r rand.Source
}

func NewScrambled() *Scrambled {
	return &Scrambled{r: rand.NewSource(time.Now().UnixNano())}
}

func (s *Scrambled) Read(bs []byte) {
	for i := len(bs) - 1; i >= 0; {
		v := s.r.Int63()
		for j := 7; i >= 0 && j >= 0; j-- {
			bs[i] = byte(v)
			i--
			v >>= 8
		}
	}
}

func OnlyLogIf(err error) {
	if err != nil {
		log.Println(err)
	}
}

func omg(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func pipelineSet(id string, count int, pipecount int, value []byte, tc *tls.Config, network string, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var conn redis.Conn
	if tc != nil {
		log.Println("using tls")
		conn, err = redis.Dial(network, addr, redis.DialNetDial(func(network, addr string) (net.Conn, error) {
			return tls.Dial(network, addr, tc)
		}))
	} else {
		conn, err = redis.Dial(network, addr)
	}
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	i := 1
	for {
		setcount := 0
		for p := 1; p <= pipecount; p++ {
			err = conn.Send("SET", fmt.Sprintf("%s-%d", id, i), value)
			OnlyLogIf(err)
			i++
			setcount++
			if i > count {
				break
			}
		}
		err = conn.Flush()
		OnlyLogIf(err)
		for r := 1; r <= setcount; r++ {
			_, err := conn.Receive()
			if err != nil {
				log.Panic(err)
			}
		}
		setcount = 0
		if i > count {
			break
		}
	}
}

func pipelineGet(id string, count int, pipecount int, value []byte, tc *tls.Config, network string, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var conn redis.Conn
	if tc != nil {
		log.Println("using tls")
		conn, err = redis.Dial(network, addr, redis.DialNetDial(func(network, addr string) (net.Conn, error) {
			return tls.Dial(network, addr, tc)
		}))
	} else {
		conn, err = redis.Dial(network, addr)
	}
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	i := 1
	for {
		getcount := 0
		for p := 1; p <= pipecount; p++ {
			err = conn.Send("GET", fmt.Sprintf("%s-%d", id, i))
			OnlyLogIf(err)
			i++
			getcount++
			if i > count {
				break
			}
		}
		err = conn.Flush()
		OnlyLogIf(err)
		for r := 1; r <= getcount; r++ {
			_, err := conn.Receive()
			if err != nil {
				log.Panic(err)
			}
		}
		getcount = 0
		if i > count {
			break
		}
	}
}

func clientSetWorker(id string, count int, value []byte, tc *tls.Config, network string, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var conn redis.Conn
	if tc != nil {
		log.Println("using tls")
		conn, err = redis.Dial(network, addr, redis.DialNetDial(func(network, addr string) (net.Conn, error) {
			return tls.Dial(network, addr, tc)
		}))
	} else {
		conn, err = redis.Dial(network, addr)
	}
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	for i := 0; i <= count; i++ {
		_, err := conn.Do("SET", fmt.Sprintf("%s-%d", id, i), value)
		if err != nil {
			log.Panic(err)
		}
	}
}

func clientGetWorker(id string, count int, value []byte, tc *tls.Config, network string, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var conn redis.Conn
	if tc != nil {
		log.Println("using tls")
		conn, err = redis.Dial(network, addr, redis.DialNetDial(func(network, addr string) (net.Conn, error) {
			return tls.Dial(network, addr, tc)
		}))
	} else {
		conn, err = redis.Dial(network, addr)
	}
	if err != nil {
		log.Panic(err)
	}
	defer conn.Close()
	for i := 1; i <= count; i++ {
		v, err := redis.Bytes(conn.Do("GET", fmt.Sprintf("%s-%d", id, i)))
		if err != nil {
			log.Printf("Error on GET %s: %v\n", fmt.Sprintf("%s-%d", id, i), err)
		}

		if !bytes.Equal(v, value) {
			log.Printf("Value Missmatch or missing on %s", fmt.Sprintf("%s-%d", id, i))
		}
	}
}

func newClientTLSFromFile(certFile, serverName string, SkipVerify bool) (*tls.Config, error) {
	b, err := ioutil.ReadFile(certFile)
	if err != nil {
		return &tls.Config{}, err
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return &tls.Config{}, fmt.Errorf("failed to append certificates for client ca store")
	}
	return &tls.Config{ServerName: serverName, RootCAs: cp, InsecureSkipVerify: SkipVerify}, nil
}

var (
	pool        *redis.Pool
	redisServer = flag.String("redisServer", "localhost:6379", "")
)

func main() {
	num := flag.Int("num", 1000000, "# of entries")
	vsize := flag.Int("vsize", 128, "value size")
	procs := flag.Int("procs", 1, "gomaxprocs count")
	clients := flag.Int("clients", 1, "# of client workers to spawn")
	pipeTest := flag.Int("pipeline", 0, "set to non zero to pipeline")
	setTest := flag.Bool("settest", false, "do set test")
	getTest := flag.Bool("gettest", false, "do get test")
	certFile := flag.String("certfile", "/etc/oort/server.crt", "")
	serverName := flag.String("servername", "localhost", "")
	skipVerify := flag.Bool("skipverify", true, "")
	useTLS := flag.Bool("usetls", false, "")
	flag.Parse()
	runtime.GOMAXPROCS(*procs)

	s := NewScrambled()
	value := make([]byte, *vsize)
	s.Read(value)
	perClient := *num / *clients

	var err error
	var tlsClientConfig *tls.Config
	if *useTLS {
		tlsClientConfig, err = newClientTLSFromFile(*certFile, *serverName, *skipVerify)
		if err != nil {
			panic(err)
		}
	}

	if *setTest {
		if *pipeTest != 0 {
			log.Printf("Spawning %d pipelined clients.\n", *clients)
		} else {
			log.Printf("Spawning %d non pipelined clients.\n", *clients)
		}
		t := time.Now()
		var wg sync.WaitGroup
		for w := 1; w <= *clients; w++ {
			wg.Add(1)
			if *pipeTest != 0 {
				go pipelineSet(fmt.Sprintf("somethingtestkey%d", w), perClient, *pipeTest, value, tlsClientConfig, "tcp", *redisServer, &wg)
			} else {
				go clientSetWorker(fmt.Sprintf("somethingtestkey%d", w), perClient, value, tlsClientConfig, "tcp", *redisServer, &wg)
			}
		}
		wg.Wait()
		log.Println("Issued", *num, "SETS")
		ts := time.Since(t).Seconds()
		log.Println(time.Since(t).Nanoseconds()/int64(*num), "ns/op")
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*num)/ts)
	}
	if *getTest {
		log.Println()
		if *pipeTest != 0 {
			log.Printf("Spawning %d pipelined clients.\n", *clients)

		} else {
			log.Printf("Spawning %d non pipelined clients.\n", *clients)
		}
		t := time.Now()
		var wg sync.WaitGroup
		for w := 1; w <= *clients; w++ {
			wg.Add(1)
			if *pipeTest != 0 {
				go pipelineGet(fmt.Sprintf("somethingtestkey%d", w), perClient, *pipeTest, value, tlsClientConfig, "tcp", *redisServer, &wg)
			} else {
				go clientGetWorker(fmt.Sprintf("somethingtestkey%d", w), perClient, value, tlsClientConfig, "tcp", *redisServer, &wg)
			}
		}
		wg.Wait()
		log.Println("Issued", *num, "GETS")
		ts := time.Since(t).Seconds()
		log.Println(time.Since(t).Nanoseconds()/int64(*num), "ns/op")
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*num)/ts)
	}
	return
}
