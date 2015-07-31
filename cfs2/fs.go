package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	pb "github.com/pandemicsyn/ort/api/proto"
	"google.golang.org/grpc"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
)

type server struct {
	fs *fs
	wg sync.WaitGroup
}

func newserver(fs *fs) *server {
	s := &server{
		fs: fs,
	}
	return s
}

func (s *server) serve() error {
	defer s.wg.Wait()

	for {
		req, err := s.fs.conn.ReadRequest()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.fs.handle(req)
		}()
	}
	return nil
}

type fs struct {
	conn *fuse.Conn
	rpc  *rpc
}

func newfs(c *fuse.Conn, r *rpc) *fs {
	fs := &fs{
		conn: c,
		rpc:  r,
	}
	return fs
}

// Handle fuse request
func (f *fs) handle(r fuse.Request) {
	switch r := r.(type) {
	default:
		log.Printf("Unhandled request: %v", r)
		r.RespondError(fuse.ENOSYS)

	case *fuse.GetattrRequest:
		f.handleGetattr(r)

	case *fuse.LookupRequest:
		f.handleLookup(r)

	case *fuse.MkdirRequest:
		f.handleMkdir(r)

	case *fuse.OpenRequest:
		f.handleOpen(r)

	case *fuse.ReadRequest:
		f.handleRead(r)
		/*
			case *fuse.InitRequest:
				f.handleInit(r)

			case *fuse.StatfsRequest:
				f.handleStatfs(r)

			case *fuse.SetattrRequest:
				f.handleSetattr(r)

			case *fuse.SymlinkRequest:
				f.handleSymlink(r)

			case *fuse.ReadlinkRequest:
				f.handleReadlink(r)

			case *fuse.LinkRequest:
				f.handleLink(r)

			case *fuse.RemoveRequest:
				f.handleRemove(r)

			case *fuse.AccessRequest:
				f.handleAccess(r)

			case *fuse.CreateRequest:
				f.handleCreate(r)

			case *fuse.GetxattrRequest:
				f.handleGetxattr(r)

			case *fuse.ListxattrRequest:
				f.handleListxattr(r)

			case *fuse.SetxattrRequest:
				f.handleSetxattr(r)

			case *fuse.RemovexattrRequest:
				f.handleRemovexattr(r)

			case *fuse.ForgetRequest:
				f.handleForget(r)

			case *fuse.WriteRequest:
				f.handleWrite(r)

			case *fuse.FlushRequest:
				f.handleFlush(r)

			case *fuse.ReleaseRequest:
				f.handleRelease(r)

			case *fuse.DestroyRequest:
				f.handleDestroy(r)

			case *fuse.RenameRequest:
				f.handleRename(r)

			case *fuse.MknodRequest:
				f.handleMknod(r)

			case *fuse.FsyncRequest:
				f.handleFsync(r)

			case *fuse.InterruptRequest:
				f.handleInterrupt(r)
		*/
	}
}

// Note: All handle functions should call r.Respond or r.Respond error before returning

func (f *fs) handleGetattr(r *fuse.GetattrRequest) {
	log.Println("Inside handleGetattr")
	log.Println(r)
	resp := &fuse.GetattrResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	a, err := f.rpc.api.GetAttr(rctx, &pb.FileRequest{Inode: uint64(r.Node)})
	if err != nil {
		log.Fatalf("GetAttr fail: %v", err)
	}
	resp.Attr.Mode = os.FileMode(a.Mode)
	resp.Attr.Size = a.Size
	resp.Attr.Mtime = time.Unix(a.Mtime, 0)

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleLookup(r *fuse.LookupRequest) {
	log.Println("Inside handleLookup")
	log.Printf("Running Lookup for %s", r.Name)
	log.Println(r)
	resp := &fuse.LookupResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	l, err := f.rpc.api.Lookup(rctx, &pb.LookupRequest{Name: r.Name, Parent: uint64(r.Node)})

	if err != nil {
		log.Fatalf("Lookup failed(%s): %v", r.Name, err)
	}
	// If there is no name then it wasn't found
	if l.Name != r.Name {
		log.Printf("ENOENT Lookup(%s)", r.Name)
		r.RespondError(fuse.ENOENT)
		return
	}
	resp.Node = fuse.NodeID(l.Attr.Inode)
	resp.Attr.Inode = l.Attr.Inode
	resp.Attr.Mode = os.FileMode(l.Attr.Mode)
	resp.Attr.Size = l.Attr.Size
	resp.Attr.Mtime = time.Unix(l.Attr.Mtime, 0)
	resp.Attr.Atime = time.Unix(l.Attr.Atime, 0)
	resp.Attr.Ctime = time.Unix(l.Attr.Ctime, 0)
	resp.Attr.Crtime = time.Unix(l.Attr.Crtime, 0)
	resp.EntryValid = 5 * time.Second

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleMkdir(r *fuse.MkdirRequest) {
	log.Println("Inside handleMkdir")
	log.Println(r)
	resp := &fuse.MkdirResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	m, err := f.rpc.api.MkDir(rctx, &pb.DirEnt{Name: r.Name, Parent: uint64(r.Node)})
	if err != nil {
		log.Fatalf("Mkdir failed(%s): %v", r.Name, err)
	}
	// If the name is empty, then the dir already exists
	if m.Name != r.Name {
		log.Printf("EEXIST Mkdir(%s)", r.Name)
		r.RespondError(fuse.EEXIST)
		return
	}
	resp.Node = fuse.NodeID(m.Attr.Inode)
	resp.Attr.Inode = m.Attr.Inode
	resp.Attr.Mode = os.FileMode(m.Attr.Mode)
	resp.Attr.Size = m.Attr.Size
	resp.Attr.Mtime = time.Unix(m.Attr.Mtime, 0)
	resp.Attr.Atime = time.Unix(m.Attr.Atime, 0)
	resp.Attr.Ctime = time.Unix(m.Attr.Ctime, 0)
	resp.Attr.Crtime = time.Unix(m.Attr.Crtime, 0)
	resp.EntryValid = 5 * time.Second

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleOpen(r *fuse.OpenRequest) {
	log.Println("Inside handleOpen")
	log.Println(r)
	resp := &fuse.OpenResponse{}
	// TODO: Figure out what to do for file handles
	// For now use the inode as the file handle
	resp.Handle = fuse.HandleID(r.Node)
	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleRead(r *fuse.ReadRequest) {
	log.Println("Inside handleRead")
	log.Println(r)
	resp := &fuse.ReadResponse{Data: make([]byte, 0, r.Size)}
	if r.Dir {
		// handle directory listing
		rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		d, err := f.rpc.api.ReadDirAll(rctx, &pb.DirRequest{Inode: uint64(r.Node)})
		if err != nil {
			log.Fatalf("Read on dir failed: %v", err)
		}
		log.Println(d.DirEntries)
		var data []byte
		data = fuse.AppendDirent(data, fuse.Dirent{
			Name:  ".",
			Inode: uint64(r.Node),
			Type:  fuse.DT_Dir,
		})
		data = fuse.AppendDirent(data, fuse.Dirent{
			Name: "..",
			Type: fuse.DT_Dir,
		})
		for _, de := range d.DirEntries {
			log.Println(de)
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  de.Name,
				Inode: de.Attr.Inode,
				Type:  fuse.DT_Dir,
			})
		}
		for _, fe := range d.FileEntries {
			log.Println(fe)
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  fe.Name,
				Inode: fe.Attr.Inode,
				Type:  fuse.DT_File,
			})
		}
		fuseutil.HandleRead(r, resp, data)
		r.Respond(resp)
		return
	} else {
		// handle file read
		// TODO: implement Read on files
		log.Println("Read for files not implemented yet")
		r.RespondError(fuse.ENOSYS)
	}
}

// TODO: Implement the following functions (and make sure to comment out the case)

func (f *fs) handleInit(r *fuse.InitRequest) {
	log.Println("Inside handleInit")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleStatfs(r *fuse.StatfsRequest) {
	log.Println("Inside handleStatfs")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleSetattr(r *fuse.SetattrRequest) {
	log.Println("Inside handleSetattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleSymlink(r *fuse.SymlinkRequest) {
	log.Println("Inside handleSymlink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleReadlink(r *fuse.ReadlinkRequest) {
	log.Println("Inside handleReadlink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleLink(r *fuse.LinkRequest) {
	log.Println("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleAccess(r *fuse.AccessRequest) {
	log.Println("Inside handleAccess")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRemove(r *fuse.RemoveRequest) {
	log.Println("Inside handleRemove")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleCreate(r *fuse.CreateRequest) {
	log.Println("Inside handleCreate")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleGetxattr(r *fuse.GetxattrRequest) {
	log.Println("Inside handleGetxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleListxattr(r *fuse.ListxattrRequest) {
	log.Println("Inside handleListxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleSetxattr(r *fuse.SetxattrRequest) {
	log.Println("Inside handleSetxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRemovexattr(r *fuse.RemovexattrRequest) {
	log.Println("Inside handleRemovexattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleForget(r *fuse.ForgetRequest) {
	log.Println("Inside handleForget")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleWrite(r *fuse.WriteRequest) {
	log.Println("Inside handleWrite")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleFlush(r *fuse.FlushRequest) {
	log.Println("Inside handleFlush")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRelease(r *fuse.ReleaseRequest) {
	log.Println("Inside handleRelease")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleDestroy(r *fuse.DestroyRequest) {
	log.Println("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRename(r *fuse.RenameRequest) {
	log.Println("Inside handleRename")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleMknod(r *fuse.MknodRequest) {
	log.Println("Inside handleMknod")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleFsync(r *fuse.FsyncRequest) {
	log.Println("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleInterrupt(r *fuse.InterruptRequest) {
	log.Println("Inside handleInterrupt")
	r.RespondError(fuse.ENOSYS)
}

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

func debuglog(msg interface{}) {
	fmt.Fprintf(os.Stderr, "%v\n", msg)
}

type rpc struct {
	conn *grpc.ClientConn
	api  pb.ApiClient
}

func newrpc(conn *grpc.ClientConn) *rpc {
	r := &rpc{
		conn: conn,
		api:  pb.NewApiClient(conn),
	}

	return r
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}

	// Setup grpc
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

	rpc := newrpc(conn)
	fs := newfs(c, rpc)
	srv := newserver(fs)

	if err := srv.serve(); err != nil {
		log.Fatal(err)
	}

	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
