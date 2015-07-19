package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"

	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Dir struct {
	sync.RWMutex
	attr   fuse.Attr
	path   string
	fs     *CFS
	parent *Dir
	nodes  map[string]fs.Node
}

//doneish
func (d *Dir) Attr(ctx context.Context, o *fuse.Attr) error {
	d.RLock()
	grpclog.Printf("Getting attrs for %s", d.path)

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	a, err := d.fs.dc.GetAttr(rctx, &pb.DirRequest{Name: d.path})
	if err != nil {
		grpclog.Fatalf("%v.GetAttr(_) = _, %v: ", d.fs.dc, err)
	}
	d.attr.Mode = os.FileMode(a.Mode)
	d.attr.Size = a.Size
	d.attr.Mtime = time.Unix(a.Mtime, 0)
	*o = d.attr
	d.RUnlock()
	return nil
}

func (d *Dir) genFsNode(a *pb.DirAttr) fs.Node {
	return &Dir{
		attr: fuse.Attr{
			Inode:  a.Inode,
			Atime:  time.Unix(a.Atime, 0),
			Mtime:  time.Unix(a.Mtime, 0),
			Ctime:  time.Unix(a.Ctime, 0),
			Crtime: time.Unix(a.Crtime, 0),
			Mode:   os.FileMode(a.Mode),
			Valid:  5 * time.Second,
		},
		fs:    d.fs,
		nodes: make(map[string]fs.Node),
	}
}

//doneish
func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	grpclog.Printf("Running Lookup for %s", name)

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	l, err := d.fs.dc.Lookup(rctx, &pb.DirRequest{Name: name})
	if err != nil {
		grpclog.Fatalf("%v.GetAttr(_) = _, %v: ", d.fs.dc, err)
	}
	//if our struct comes back with no name the entry wasn't found
	if l.Name != name {
		return nil, fuse.ENOENT
	}
	n := d.genFsNode(l.Attr)
	return n, nil
}

//TODO: all the things
func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.RLock()
	dirs := make([]fuse.Dirent, len(d.nodes)+2)

	// Add special references.
	dirs[0] = fuse.Dirent{
		Name:  ".",
		Inode: d.attr.Inode,
		Type:  fuse.DT_Dir,
	}
	dirs[1] = fuse.Dirent{
		Name: "..",
		Type: fuse.DT_Dir,
	}
	if d.parent != nil {
		dirs[1].Inode = d.parent.attr.Inode
	} else {
		dirs[1].Inode = d.attr.Inode
	}

	// Add remaining files.
	idx := 2
	for name, node := range d.nodes {
		ent := fuse.Dirent{
			Name: name,
		}
		switch n := node.(type) {
		case *File:
			ent.Inode = n.attr.Inode
			ent.Type = fuse.DT_File
		case *Dir:
			ent.Inode = n.attr.Inode
			ent.Type = fuse.DT_Dir
		}
		dirs[idx] = ent
		idx++
	}
	d.RUnlock()
	return dirs, nil
}
func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.Lock()
	defer d.Unlock()

	if _, exists := d.nodes[req.Name]; exists {
		return nil, fuse.EEXIST
	}

	n := d.fs.newDir(req.Mode)
	d.nodes[req.Name] = n
	atomic.AddUint64(&d.fs.nodeCount, 1)

	return n, nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.Lock()
	defer d.Unlock()

	if _, exists := d.nodes[req.Name]; exists {
		return nil, nil, fuse.EEXIST
	}

	n := d.fs.newFile(req.Mode, req.Name)
	n.fs = d.fs
	d.nodes[req.Name] = n
	atomic.AddUint64(&d.fs.nodeCount, 1)

	resp.Attr = n.attr

	return n, n, nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd := newDir.(*Dir)
	if d.attr.Inode == nd.attr.Inode {
		d.Lock()
		defer d.Unlock()
	} else if d.attr.Inode < nd.attr.Inode {
		d.Lock()
		defer d.Unlock()
		nd.Lock()
		defer nd.Unlock()
	} else {
		nd.Lock()
		defer nd.Unlock()
		d.Lock()
		defer d.Unlock()
	}

	if _, exists := d.nodes[req.OldName]; !exists {
		return fuse.ENOENT
	}

	// Rename can be used as an atomic replace, override an existing file.
	if old, exists := nd.nodes[req.NewName]; exists {
		atomic.AddUint64(&d.fs.nodeCount, ^uint64(0)) // decrement by one
		if oldFile, ok := old.(*File); !ok {
			atomic.AddInt64(&d.fs.size, -int64(oldFile.attr.Size))
		}
	}

	nd.nodes[req.NewName] = d.nodes[req.OldName]
	delete(d.nodes, req.OldName)
	return nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.Lock()
	defer d.Unlock()

	if n, exists := d.nodes[req.Name]; !exists {
		return fuse.ENOENT
	} else if req.Dir && len(n.(*Dir).nodes) > 0 {
		return fuse.Errno(syscall.ENOTEMPTY)
	}

	delete(d.nodes, req.Name)
	atomic.AddUint64(&d.fs.nodeCount, ^uint64(0)) // decrement by one
	return nil
}
