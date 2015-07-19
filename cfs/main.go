//cfs implements an test hybrid in-memory/gstore backed file system.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	pb "github.com/pandemicsyn/ort/api/proto"
	"google.golang.org/grpc"
)

var (
	debug              = flag.Bool("debug", false, "enable debug log messages to stderr")
	serverAddr         = flag.String("host", "127.0.0.1:8443", "The ort api server to connect too")
	serverHostOverride = flag.String("host_override", "localhost", "")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func debugLog(msg interface{}) {
	fmt.Fprintf(os.Stderr, "%v\n", msg)
}

type CFS struct {
	root      *Dir
	nodeID    uint64
	nodeCount uint64
	size      int64
	conn      *grpc.ClientConn
	fc        pb.FileApiClient
	dc        pb.DirApiClient
}

func NewCFS(conn *grpc.ClientConn) *CFS {
	fs := &CFS{
		nodeCount: 1,
		conn:      conn,
		fc:        pb.NewFileApiClient(conn),
		dc:        pb.NewDirApiClient(conn),
	}
	fs.root = fs.newDir(os.ModeDir|0777, "/")
	if fs.root.attr.Inode != 1 {
		panic("Root node should have been assigned id 1")
	}
	return fs
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	/* grpc setup before else incase we can't dial */
	var opts []grpc.DialOption
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	mountpoint := flag.Arg(0)
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("cfs"),
		fuse.Subtype("cfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("CFS"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	cfg := &fs.Config{}
	if *debug {
		cfg.Debug = debugLog
	}
	srv := fs.New(c, cfg)
	filesys := NewCFS(conn)

	if err := srv.Serve(filesys); err != nil {
		log.Fatal(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

/*
// Compile-time interface checks.
var _ fs.FS = (*CFS)(nil)
var _ fs.FSStatfser = (*CFS)(nil)

var _ fs.Node = (*Dir)(nil)
var _ fs.NodeCreater = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)
var _ fs.NodeRenamer = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)

var _ fs.HandleReadAller = (*File)(nil)
var _ fs.HandleWriter = (*File)(nil)
var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)
*/

func (m *CFS) nextID() uint64 {
	return atomic.AddUint64(&m.nodeID, 1)
}

func (m *CFS) newDir(mode os.FileMode, name string) *Dir {
	n := time.Now()
	return &Dir{
		path: name,
		attr: fuse.Attr{
			Inode:  m.nextID(),
			Atime:  n,
			Mtime:  n,
			Ctime:  n,
			Crtime: n,
			Mode:   os.ModeDir | mode,
			Valid:  5 * time.Second,
		},
		fs:    m,
		nodes: make(map[string]fs.Node),
	}
}

func (m *CFS) newFile(mode os.FileMode, name string) *File {
	n := time.Now()
	return &File{
		path: name,
		attr: fuse.Attr{
			Inode:  m.nextID(),
			Atime:  n,
			Mtime:  n,
			Ctime:  n,
			Crtime: n,
			Mode:   mode,
			Valid:  10 * time.Second,
		},
		data: make([]byte, 0),
	}
}

func (m *CFS) Root() (fs.Node, error) {
	return m.root, nil
}

func (m *CFS) Statfs(ctx context.Context, req *fuse.StatfsRequest,
	resp *fuse.StatfsResponse) error {
	resp.Blocks = uint64((atomic.LoadInt64(&m.size) + 511) / 512)
	resp.Bsize = 512
	resp.Files = atomic.LoadUint64(&m.nodeCount)
	return nil
}
