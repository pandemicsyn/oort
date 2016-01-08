package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gholt/brimtime"
	vp "github.com/pandemicsyn/oort/api/valueproto"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

func grpcStreamWrite(id string, count int, value []byte, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	w := &vp.WriteRequest{
		Value: value,
	}
	stream, err := client.StreamWrite(context.Background())
	for i := 1; i <= count; i++ {
		w.KeyA, w.KeyB = murmur3.Sum128([]byte(fmt.Sprintf("%s-%d", id, i)))
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		if err := stream.Send(w); err != nil {
			log.Println(err)
			continue
		}
		res, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}
		if res.Tsm > w.Tsm {
			log.Printf("TSM is newer than attempted, Key %s-%d Got %s, Sent: %s", id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(w.Tsm))
		}
	}
	stream.CloseSend()
}

func grpcWrite(id string, count int, value []byte, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	w := &vp.WriteRequest{
		Value: value,
	}
	for i := 1; i <= count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		w.KeyA, w.KeyB = murmur3.Sum128([]byte(fmt.Sprintf("%s-%d", id, i)))
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		res, err := client.Write(ctx, w)
		if err != nil {
			log.Println("Client", id, ":", err)
		}
		if res.Tsm > w.Tsm {
			log.Printf("TSM is newer than attempted, Key %s-%d Got %s, Sent: %s", id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(w.Tsm))
		}
	}
}

func grpcRead(id string, count int, value []byte, addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	r := &vp.ReadRequest{}
	for i := 1; i <= count; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		r.KeyA, r.KeyB = murmur3.Sum128([]byte(fmt.Sprintf("%s-%d", id, i)))
		r.Tsm = brimtime.TimeToUnixMicro(time.Now())
		res, err := client.Read(ctx, r)
		if err != nil {
			log.Println("Client", id, ":", err)
		}
		if res.Tsm != r.Tsm {
			log.Printf("TSM missmatch, Key %s-%d Got %s, Sent: %s", id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(r.Tsm))
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
	setTest := flag.Bool("settest", false, "do set test")
	getTest := flag.Bool("gettest", false, "do get test")
	flag.Parse()
	runtime.GOMAXPROCS(*procs)

	s := NewScrambled()
	value := make([]byte, *vsize)
	s.Read(value)
	perClient := *num / *clients

	if *setTest {
		t := time.Now()
		var wg sync.WaitGroup
		for w := 1; w <= *clients; w++ {
			wg.Add(1)
			go grpcWrite(fmt.Sprintf("somethingtestkey%d", w), perClient, value, *redisServer, &wg)
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
		t := time.Now()
		var wg sync.WaitGroup
		for w := 1; w <= *clients; w++ {
			wg.Add(1)
			go grpcRead(fmt.Sprintf("somethingtestkey%d", w), perClient, value, *redisServer, &wg)
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
