package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gholt/brimtime"
	gp "github.com/pandemicsyn/oort/api/groupproto"
	vp "github.com/pandemicsyn/oort/api/valueproto"
	"github.com/peterh/liner"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var vaddr = flag.String("vhost", "127.0.0.1:6379", "vstore addr")
var gaddr = flag.String("ghost", "127.0.0.1:6380", "gstore addr")
var groupmode = flag.Bool("g", false, "whether we're talking to a groupstore instance")

var (
	prompt    = "> "
	errprompt = "┻━┻ ︵ヽ(`Д´)ﾉ︵ ┻━┻> "
	historyf  = filepath.Join(os.TempDir(), ".oort-cli-history")
	cmdnames  = []string{"write", "write-hash", "read", "read-hash", "delete", "lookup", "lookup-group", "mode", "exit", "help"}
)

func lineCompleter(line string) (c []string) {
	for _, n := range cmdnames {
		if strings.HasPrefix(n, strings.ToLower(line)) {
			c = append(c, n)
		}
	}
	return
}

func (c *Client) printHelp() string {
	if c.gmode {
		return fmt.Sprintf(`
	Valid cmd's are:
	write <groupkey> <subkey> <some string value>
	write-hash <groupkey> <subkeyhasha> <subkeyhashb> <value>
	read <groupkey> <subkey>
	read-hash <groupkey> <subkeyhasha> <subkeyhashb>
	delete <groupkey> <subkey>
	lookup <groupkey> <subkey>
	lookup-group <key>
	mode group|value
	exit
	help
	`)
	} else {
		return fmt.Sprintf(`
	Valid cmd's are:
	write <key> <some string value>
	read <key>
	delete <key>
	lookup <key>
	mode group|value
	exit
	help
	`)
	}
}

func (c *Client) parseValueCmd(line string) (string, error) {
	if c.vconn == nil {
		c.getValueClient()
	}
	split := strings.SplitN(line, " ", 2)
	cmd := split[0]
	if len(split) != 2 {
		if cmd == "exit" {
			return "", fmt.Errorf("Exiting..")
		}
		if cmd == "help" {
			return c.printHelp(), nil
		}
		return c.printHelp(), nil
	}
	args := split[1]
	switch cmd {
	case "write":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("write needs key and value: `write somekey some value thing here`"), nil
		}
		w := &vp.WriteRequest{}
		w.KeyA, w.KeyB = murmur3.Sum128([]byte(sarg[0]))
		w.Value = []byte(sarg[1])
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.vc.Write(ctx, w)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TSM: %d\nTSM: %d", w.Tsm, res.Tsm), nil
	case "read":
		r := &vp.ReadRequest{}
		r.KeyA, r.KeyB = murmur3.Sum128([]byte(args))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.vc.Read(ctx, r)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d\nVALUE: %s", res.Tsm, res.Value), nil
	case "delete":
		d := &vp.DeleteRequest{}
		d.KeyA, d.KeyB = murmur3.Sum128([]byte(args))
		d.Tsm = brimtime.TimeToUnixMicro(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.vc.Delete(ctx, d)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d", res.Tsm), nil
	case "lookup":
		l := &vp.LookupRequest{}
		l.KeyA, l.KeyB = murmur3.Sum128([]byte(args))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.vc.Lookup(ctx, l)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d", res.Tsm), nil
	case "mode":
		if args == "value" {
			return fmt.Sprintf("Already in value store mode"), nil
		}
		if args == "group" {
			c.gmode = true
			return fmt.Sprintf("Switched to group mode"), nil
		}
		return fmt.Sprintf("Valid modes are: value | group"), nil
	case "exit":
		log.Println("exit")
		return "", fmt.Errorf("Exiting..")
	}
	return c.printHelp(), nil
}

func (c *Client) parseGroupCmd(line string) (string, error) {
	if c.gconn == nil {
		c.getGroupClient()
	}
	split := strings.SplitN(line, " ", 2)
	cmd := split[0]
	if len(split) != 2 {
		if cmd == "exit" {
			return "", fmt.Errorf("Exiting..")
		}
		if cmd == "help" {
			return c.printHelp(), nil
		}
		return c.printHelp(), nil
	}
	args := split[1]
	switch cmd {
	case "write":
		sarg := strings.SplitN(args, " ", 3)
		if len(sarg) < 3 {
			return fmt.Sprintf("write needs groupkey, key, value: `write groupkey somekey some value thing here`"), nil
		}
		w := &gp.WriteRequest{}
		w.KeyA, w.KeyB = murmur3.Sum128([]byte(sarg[0]))
		w.NameKeyA, w.NameKeyB = murmur3.Sum128([]byte(sarg[1]))
		w.Value = []byte(sarg[2])
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Write(ctx, w)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TSM: %d\nTSM: %d", w.Tsm, res.Tsm), nil
	case "write-hash":
		sarg := strings.SplitN(args, " ", 4)
		if len(sarg) < 4 {
			return fmt.Sprintf("write-hash needs groupkey, keyahash keybhash, value: `write-hash groupkey 19191919 19191919 some value thing here`"), nil
		}
		w := &gp.WriteRequest{}
		w.KeyA, w.KeyB = murmur3.Sum128([]byte(sarg[0]))
		namekeyA, err := strconv.ParseUint(sarg[1], 10, 64)
		if err != nil {
			return "", err
		}
		namekeyB, err := strconv.ParseUint(sarg[2], 10, 64)
		if err != nil {
			return "", err
		}
		w.NameKeyA, w.NameKeyB = namekeyA, namekeyB
		w.Value = []byte(sarg[3])
		w.Tsm = brimtime.TimeToUnixMicro(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Write(ctx, w)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TSM: %d\nTSM: %d", w.Tsm, res.Tsm), nil
	case "read":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("read needs groupkey, subkey"), nil
		}
		r := &gp.ReadRequest{}
		r.KeyA, r.KeyB = murmur3.Sum128([]byte(sarg[0]))
		r.NameKeyA, r.NameKeyB = murmur3.Sum128([]byte(sarg[1]))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Read(ctx, r)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d\nVALUE: %s", res.Tsm, res.Value), nil
	case "read-hash":
		sarg := strings.SplitN(args, " ", 3)
		if len(sarg) < 3 {
			return fmt.Sprintf("read needs groupkey, subkeyA, subkeyB"), nil
		}
		r := &gp.ReadRequest{}
		r.KeyA, r.KeyB = murmur3.Sum128([]byte(sarg[0]))
		namekeyA, err := strconv.ParseUint(sarg[1], 10, 64)
		if err != nil {
			return "", err
		}
		namekeyB, err := strconv.ParseUint(sarg[2], 10, 64)
		if err != nil {
			return "", err
		}
		r.NameKeyA, r.NameKeyB = namekeyA, namekeyB
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Read(ctx, r)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d\nVALUE: %s", res.Tsm, res.Value), nil
	case "delete":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("delete needs groupkey, subkey"), nil
		}
		d := &gp.DeleteRequest{}
		d.KeyA, d.KeyB = murmur3.Sum128([]byte(sarg[0]))
		d.NameKeyA, d.NameKeyB = murmur3.Sum128([]byte(sarg[1]))
		d.Tsm = brimtime.TimeToUnixMicro(time.Now())
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Delete(ctx, d)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d", res.Tsm), nil
	case "lookup":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("lookup needs groupkey, subkey"), nil
		}
		l := &gp.LookupRequest{}
		l.KeyA, l.KeyB = murmur3.Sum128([]byte(sarg[0]))
		l.NameKeyA, l.NameKeyB = murmur3.Sum128([]byte(sarg[1]))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.Lookup(ctx, l)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TSM: %d", res.Tsm), nil
	case "lookup-group":
		l := &gp.LookupGroupRequest{}
		l.A, l.B = murmur3.Sum128([]byte(args))
		ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		res, err := c.gc.LookupGroup(ctx, l)
		if err != nil {
			return "", err
		}
		keys := make([]string, len(res.Items))
		for k, v := range res.Items {
			keys[k] = fmt.Sprintf("TSM: %d [ %d | %d ]", v.TimestampMicro, v.NameKeyA, v.NameKeyB)
		}
		return fmt.Sprintf(strings.Join(keys, "\n")), nil
	case "mode":
		if args == "value" {
			c.gmode = false
			return fmt.Sprintf("Switched to value mode"), nil
		}
		if args == "group" {
			return fmt.Sprintf("Already in group store mode"), nil
		}
		return fmt.Sprintf("Valid modes are: value | group"), nil
	case "exit":
		log.Println("exit")
		return "", fmt.Errorf("Exiting..")
	}
	return c.printHelp(), nil
}

func (c *Client) getValueClient() {
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	var err error
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	c.vconn, err = grpc.Dial(c.vaddr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	c.vc = vp.NewValueStoreClient(c.vconn)
}

func (c *Client) getGroupClient() {
	var opts []grpc.DialOption
	var creds credentials.TransportAuthenticator
	var err error
	creds = credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})
	opts = append(opts, grpc.WithTransportCredentials(creds))
	c.gconn, err = grpc.Dial(c.gaddr, opts...)
	if err != nil {
		log.Fatalln(fmt.Sprintf("Failed to dial server: %s", err))
	}
	c.gc = gp.NewGroupStoreClient(c.gconn)
}

type Client struct {
	vaddr string
	gaddr string
	gmode bool
	vc    vp.ValueStoreClient
	vconn *grpc.ClientConn
	gc    gp.GroupStoreClient
	gconn *grpc.ClientConn
}

func main() {
	flag.Parse()
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	line.SetCompleter(lineCompleter)
	if f, err := os.Open(historyf); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	client := Client{
		vaddr: *vaddr,
		gaddr: *gaddr,
		gmode: *groupmode,
	}
	sm := "value"
	if client.gmode {
		sm = "group"
	}
	fmt.Printf("\u2728 oort-cli - in %s mode \u2728\n\n", sm)
	for {
		if cmd, err := line.Prompt(prompt); err == nil {
			if client.gmode {
				res, err := client.parseGroupCmd(cmd)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Println(res)
				line.AppendHistory(cmd)
			} else {
				res, err := client.parseValueCmd(cmd)
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				fmt.Println(res)
				line.AppendHistory(cmd)
			}
		} else if err == liner.ErrPromptAborted {
			log.Print("Aborted")
			return
		} else {
			log.Print("Error reading line: ", err)
			return
		}
		if f, err := os.Create(historyf); err != nil {
			log.Print("Error writing history file: ", err)
		} else {
			line.WriteHistory(f)
			f.Close()
		}
	}
}
