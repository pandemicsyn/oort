package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
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

func pipelineSet(id string, count int, pipecount int, value []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
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

func pipelineGet(id string, count int, pipecount int, value []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
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

func clientSetWorker(id string, count int, value []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
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

func clientGetWorker(id string, count int, value []byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
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

var (
	pool          *redis.Pool
	redisServer   = flag.String("redisServer", ":6379", "")
	redisPassword = flag.String("redisPassword", "", "")
)

func main() {
	num := flag.Int("num", 1000000, "# of entries")
	vsize := flag.Int("vsize", 128, "value size")
	procs := flag.Int("procs", 1, "gomaxprocs count")
	clients := flag.Int("clients", 1, "# of client workers to spawn")
	pipeTest := flag.Int("pipeline", 0, "set to non zero to pipeline")
	setTest := flag.Bool("settest", false, "do set test")
	getTest := flag.Bool("gettest", false, "do get test")
	flag.Parse()
	runtime.GOMAXPROCS(*procs)

	s := NewScrambled()
	value := make([]byte, *vsize)
	s.Read(value)
	perClient := *num / *clients

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
				go pipelineSet(fmt.Sprintf("somethingtestkey%d", w), perClient, *pipeTest, value, &wg)
			} else {
				go clientSetWorker(fmt.Sprintf("somethingtestkey%d", w), perClient, value, &wg)
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
				go pipelineGet(fmt.Sprintf("somethingtestkey%d", w), perClient, *pipeTest, value, &wg)
			} else {
				go clientGetWorker(fmt.Sprintf("somethingtestkey%d", w), perClient, value, &wg)
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
