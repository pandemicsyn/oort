package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gholt/brimtime"
	"github.com/gholt/store"
	"github.com/pandemicsyn/oort/api"
	"github.com/peterh/liner"
	"github.com/spaolacci/murmur3"
	"google.golang.org/grpc"
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
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		value := []byte(sarg[1])
		timestampMicro := brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err := c.vstore.Write(keyA, keyB, timestampMicro, value)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TIMESTAMPMICRO: %d\nPREVIOUS TIMESTAMPMICRO: %d", timestampMicro, oldTimestampMicro), nil
	case "read":
		keyA, keyB := murmur3.Sum128([]byte(args))
		timestampMicro, value, err := c.vstore.Read(keyA, keyB, nil)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nVALUE: %s", timestampMicro, value), nil
	case "delete":
		keyA, keyB := murmur3.Sum128([]byte(args))
		timestampMicro := brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err := c.vstore.Delete(keyA, keyB, timestampMicro)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nOLD TIMESTAMPMICRO: %d", timestampMicro, oldTimestampMicro), nil
	case "lookup":
		keyA, keyB := murmur3.Sum128([]byte(args))
		timestampMicro, length, err := c.vstore.Lookup(keyA, keyB)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nLENGTH: %d", timestampMicro, length), nil
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
	if c.gstore == nil {
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
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, childKeyB := murmur3.Sum128([]byte(sarg[1]))
		timestampMicro := brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err := c.gstore.Write(keyA, keyB, childKeyA, childKeyB, timestampMicro, []byte(sarg[2]))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TIMESTAMPMICRO: %d\nPREVIOUS TIMESTAMPMICRO: %d", timestampMicro, oldTimestampMicro), nil
	case "write-hash":
		sarg := strings.SplitN(args, " ", 4)
		if len(sarg) < 4 {
			return fmt.Sprintf("write-hash needs groupkey, keyahash keybhash, value: `write-hash groupkey 19191919 19191919 some value thing here`"), nil
		}
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, err := strconv.ParseUint(sarg[1], 10, 64)
		if err != nil {
			return "", err
		}
		childKeyB, err := strconv.ParseUint(sarg[2], 10, 64)
		if err != nil {
			return "", err
		}
		timestampMicro := brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err := c.gstore.Write(keyA, keyB, childKeyA, childKeyB, timestampMicro, []byte(sarg[3]))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("WRITE TIMESTAMPMICRO: %d\n PREVIOUS TIMESTAMPMICRO: %d", timestampMicro, oldTimestampMicro), nil
	case "read":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("read needs groupkey, subkey"), nil
		}
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, childKeyB := murmur3.Sum128([]byte(sarg[1]))
		timestampMicro, value, err := c.gstore.Read(keyA, keyB, childKeyA, childKeyB, nil)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nVALUE: %s", timestampMicro, value), nil
	case "read-hash":
		sarg := strings.SplitN(args, " ", 3)
		if len(sarg) < 3 {
			return fmt.Sprintf("read needs groupkey, subkeyA, subkeyB"), nil
		}
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, err := strconv.ParseUint(sarg[1], 10, 64)
		if err != nil {
			return "", err
		}
		childKeyB, err := strconv.ParseUint(sarg[2], 10, 64)
		if err != nil {
			return "", err
		}
		timestampMicro, value, err := c.gstore.Read(keyA, keyB, childKeyA, childKeyB, nil)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nVALUE: %s", timestampMicro, value), nil
	case "delete":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("delete needs groupkey, subkey"), nil
		}
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, childKeyB := murmur3.Sum128([]byte(sarg[1]))
		timestampMicro := brimtime.TimeToUnixMicro(time.Now())
		oldTimestampMicro, err := c.gstore.Delete(keyA, keyB, childKeyA, childKeyB, timestampMicro)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nOLD TIMESTAMPMICRO: %d", timestampMicro, oldTimestampMicro), nil
	case "lookup":
		sarg := strings.SplitN(args, " ", 2)
		if len(sarg) < 2 {
			return fmt.Sprintf("lookup needs groupkey, subkey"), nil
		}
		keyA, keyB := murmur3.Sum128([]byte(sarg[0]))
		childKeyA, childKeyB := murmur3.Sum128([]byte(sarg[1]))
		timestampMicro, length, err := c.gstore.Lookup(keyA, keyB, childKeyA, childKeyB)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		return fmt.Sprintf("TIMESTAMPMICRO: %d\nLENGTH: %d", timestampMicro, length), nil
	case "lookup-group":
		keyA, keyB := murmur3.Sum128([]byte(args))
		items, err := c.gstore.LookupGroup(keyA, keyB)
		if err == store.ErrNotFound {
			return fmt.Sprintf("not found"), nil
		} else if err != nil {
			return "", err
		}
		keys := make([]string, len(items))
		for k, v := range items {
			keys[k] = fmt.Sprintf("TIMESTAMPMICRO: %d [ %d | %d ]", v.TimestampMicro, v.ChildKeyA, v.ChildKeyB)
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
	var err error
	c.vstore, err = api.NewValueStore(c.vaddr, 10, true)
	if err != nil {
		log.Fatalln("Cannot create value store:", err)
	}
}

func (c *Client) getGroupClient() {
	var err error
	c.gstore, err = api.NewGroupStore(c.gaddr, 10, true)
	if err != nil {
		log.Fatalln("Cannot create group store:", err)
	}
}

type Client struct {
	vaddr  string
	gaddr  string
	gmode  bool
	vconn  *grpc.ClientConn
	vstore store.ValueStore
	gstore api.GroupStore
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
