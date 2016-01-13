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

	"github.com/gholt/brimtime"
	gp "github.com/pandemicsyn/oort/api/groupproto"
	vp "github.com/pandemicsyn/oort/api/valueproto"
	"github.com/pkg/profile"
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

type ValueClientConfig struct {
	id    int
	count int
	wm    []*vp.WriteRequest
	rm    []*vp.ReadRequest
	value *[]byte
	addr  string
	wg    *sync.WaitGroup
}

type GroupClientConfig struct {
	id    int
	count int
	wm    []*gp.WriteRequest
	rm    []*gp.ReadRequest
	value *[]byte
	addr  string
	wg    *sync.WaitGroup
}

func ValueStreamWrite(c *ValueClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	empty := []byte("")
	stream, err := client.StreamWrite(context.Background())

	for i, _ := range c.wm {
		c.wm[i].Value = *c.value
		if err := stream.Send(c.wm[i]); err != nil {
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
		if res.Tsm > c.wm[i].Tsm {
			log.Printf("TSM is newer than attempted, Key %d-%d Got %s, Sent: %s", c.id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(c.wm[i].Tsm))
		}
		c.wm[i].Value = empty
	}
	stream.CloseSend()
}

func GroupStreamWrite(c *GroupClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := gp.NewGroupStoreClient(conn)
	empty := []byte("")
	stream, err := client.StreamWrite(context.Background())

	for i, _ := range c.wm {
		c.wm[i].Value = *c.value
		if err := stream.Send(c.wm[i]); err != nil {
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
		if res.Tsm > c.wm[i].Tsm {
			log.Printf("TSM is newer than attempted, Key %d-%d Got %s, Sent: %s", c.id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(c.wm[i].Tsm))
		}
		c.wm[i].Value = empty
	}
	stream.CloseSend()
}

func ValueStreamRead(c *ValueClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	stream, err := client.StreamRead(context.Background())
	for i, _ := range c.rm {
		if err := stream.Send(c.rm[i]); err != nil {
			log.Println(err)
			continue
		}
		_, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}
	}
	stream.CloseSend()
}

func GroupStreamRead(c *GroupClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := gp.NewGroupStoreClient(conn)
	stream, err := client.StreamRead(context.Background())
	for i, _ := range c.rm {
		if err := stream.Send(c.rm[i]); err != nil {
			log.Println(err)
			continue
		}
		_, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}
	}
	stream.CloseSend()
}

func ValueWrite(c *ValueClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	w := &vp.WriteRequest{
		Value: *c.value,
	}
	empty := []byte("")
	for i, _ := range c.wm {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		c.wm[i].Value = *c.value
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		res, err := client.Write(ctx, c.wm[i])
		if err != nil {
			log.Println("Client", c.id, ":", err)
		}
		if res.Tsm > w.Tsm {
			log.Printf("TSM is newer than attempted, Key %d-%d Got %s, Sent: %s", c.id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(w.Tsm))
		}
		c.wm[i].Value = empty
	}
}

func GroupWrite(c *GroupClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := gp.NewGroupStoreClient(conn)
	w := &gp.WriteRequest{
		Value: *c.value,
	}
	empty := []byte("")
	for i, _ := range c.wm {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		c.wm[i].Value = *c.value
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		res, err := client.Write(ctx, c.wm[i])
		if err != nil {
			log.Println("Client", c.id, ":", err)
		}
		if res.Tsm > w.Tsm {
			log.Printf("TSM is newer than attempted, Key %d-%d Got %s, Sent: %s", c.id, i, brimtime.UnixMicroToTime(res.Tsm), brimtime.UnixMicroToTime(w.Tsm))
		}
		c.wm[i].Value = empty
	}
}

func ValueRead(c *ValueClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := vp.NewValueStoreClient(conn)
	for i, _ := range c.rm {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Read(ctx, c.rm[i])
		if err != nil {
			log.Println("Client", c.id, ":", err)
		}
	}
}

func GroupRead(c *GroupClientConfig) {
	defer c.wg.Done()
	var err error
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	conn, err := grpc.Dial(c.addr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	defer conn.Close()
	client := gp.NewGroupStoreClient(conn)
	for i, _ := range c.rm {
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Read(ctx, c.rm[i])
		if err != nil {
			log.Println("Client", c.id, ":", err)
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

func VSTests() {
	vsconfigs := make([]ValueClientConfig, *clients)
	var wg sync.WaitGroup
	for w := 0; w < *clients; w++ {
		vsconfigs[w].addr = *oortServer
		vsconfigs[w].id = w
		vsconfigs[w].count = perClient
		vsconfigs[w].value = &value
		vsconfigs[w].wg = &wg
		vsconfigs[w].wm = make([]*vp.WriteRequest, perClient)
		vsconfigs[w].rm = make([]*vp.ReadRequest, perClient)
		for k := 0; k < perClient; k++ {
			vsconfigs[w].wm[k] = &vp.WriteRequest{}
			vsconfigs[w].rm[k] = &vp.ReadRequest{}
			vsconfigs[w].wm[k].KeyA, vsconfigs[w].wm[k].KeyB = murmur3.Sum128([]byte(fmt.Sprintf("somethingtestkey%d-%d", vsconfigs[w].id, k)))
			vsconfigs[w].wm[k].Tsm = brimtime.TimeToUnixMicro(time.Now())
			vsconfigs[w].rm[k].KeyA = vsconfigs[w].wm[k].KeyA
			vsconfigs[w].rm[k].KeyB = vsconfigs[w].wm[k].KeyB
		}
	}
	log.Println("ValueStore Key/hash generation complete. Spawning tests.")

	// ValueStore Tests
	if *vsWriteTest {
		t := time.Now()
		for w := 0; w < *clients; w++ {
			wg.Add(1)
			if *streamTest {
				go ValueStreamWrite(&vsconfigs[w])
			} else {
				go ValueWrite(&vsconfigs[w])
			}
		}
		wg.Wait()
		log.Println("Issued", *clients*perClient, "VS WRITES")
		ts := time.Since(t).Seconds()
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*clients*perClient)/ts)
	}
	if *vsReadTest {
		t := time.Now()
		for w := 0; w < *clients; w++ {
			wg.Add(1)
			if *streamTest {
				go ValueStreamRead(&vsconfigs[w])
			} else {
				go ValueRead(&vsconfigs[w])
			}
		}
		wg.Wait()
		log.Println("Issued", *clients*perClient, "VS READS")
		ts := time.Since(t).Seconds()
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*clients*perClient)/ts)
	}
}

func GSTests() {
	gsconfigs := make([]GroupClientConfig, *clients)
	var wg sync.WaitGroup
	for w := 0; w < *clients; w++ {
		gsconfigs[w].addr = *oortServer
		gsconfigs[w].id = w
		gsconfigs[w].count = perClient
		gsconfigs[w].value = &value

		gsconfigs[w].wg = &wg
		gsconfigs[w].wm = make([]*gp.WriteRequest, perClient)
		gsconfigs[w].rm = make([]*gp.ReadRequest, perClient)
		perGroup := perClient / *groups
		log.Printf("Will generate %d entries per group", perGroup)
		for g := 0; g < *groups; g++ {
			grpA, grpB := murmur3.Sum128([]byte(fmt.Sprintf("group%d-%d", gsconfigs[w].id, g)))
			for k := 0; k < perGroup; k++ {
				gsconfigs[w].wm[k] = &gp.WriteRequest{}
				gsconfigs[w].rm[k] = &gp.ReadRequest{}
				gsconfigs[w].wm[k].KeyA = grpA
				gsconfigs[w].wm[k].KeyB = grpB
				gsconfigs[w].rm[k].KeyA = grpA
				gsconfigs[w].rm[k].KeyB = grpB

				gsconfigs[w].wm[k].NameKeyA, gsconfigs[w].wm[k].NameKeyB = murmur3.Sum128([]byte(fmt.Sprintf("somethingtestkey%d-%d", gsconfigs[w].id, k)))
				gsconfigs[w].wm[k].Tsm = brimtime.TimeToUnixMicro(time.Now())
				gsconfigs[w].rm[k].NameKeyA = gsconfigs[w].wm[k].KeyA
				gsconfigs[w].rm[k].NameKeyB = gsconfigs[w].wm[k].KeyB

			}
		}

	}
	log.Println("GroupStore Key/hash generation complete. Spawning tests.")

	if *gsWriteTest {
		t := time.Now()
		for w := 0; w < *clients; w++ {
			wg.Add(1)
			if *streamTest {
				go GroupStreamWrite(&gsconfigs[w])
			} else {
				go GroupWrite(&gsconfigs[w])
			}
		}
		wg.Wait()
		log.Println("Issued", *clients*perClient, "GS WRITES")
		ts := time.Since(t).Seconds()
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*clients*perClient)/ts)
	}
	if *gsReadTest {
		t := time.Now()
		for w := 0; w < *clients; w++ {
			wg.Add(1)
			if *streamTest {
				go GroupStreamRead(&gsconfigs[w])
			} else {
				go GroupRead(&gsconfigs[w])
			}
		}
		wg.Wait()
		log.Println("Issued", *clients*perClient, "GS READS")
		ts := time.Since(t).Seconds()
		log.Println("Total run time was:", ts, "seconds")
		log.Printf("Per second: %.2f\n", float64(*clients*perClient)/ts)
	}
}

var (
	num           = flag.Int("num", 1000000, "# of entries")
	vsize         = flag.Int("vsize", 128, "value size")
	procs         = flag.Int("procs", 1, "gomaxprocs count")
	clients       = flag.Int("clients", 1, "# of client workers to spawn")
	vsWriteTest   = flag.Bool("vswrite", false, "do valuestore write test")
	vsReadTest    = flag.Bool("vsread", false, "do valuestore read test")
	groups        = flag.Int("groups", 1, "# of groups to use per client")
	gsWriteTest   = flag.Bool("gswrite", false, "do groupstore write test")
	gsReadTest    = flag.Bool("gsread", false, "do groupstore read test")
	streamTest    = flag.Bool("stream", false, "use streaming api")
	profileEnable = flag.Bool("profile", false, "enable cpu profiling")
	oortServer    = flag.String("oortServer", "localhost:6379", "")
	perClient     int
	value         []byte
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)
	if *profileEnable {
		defer profile.Start().Stop()
	}
	s := NewScrambled()
	value = make([]byte, *vsize)
	s.Read(value)
	perClient = *num / *clients

	log.Println("Using streaming api:", *streamTest)

	if *vsWriteTest || *vsReadTest {
		VSTests()
	}
	if *gsWriteTest || *gsReadTest {
		GSTests()
	}
	return
}
