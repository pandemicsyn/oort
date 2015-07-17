package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"syscall"
)

type Dir struct {
	sync.RWMutex
	attr fuse.Attr

	fs     *CFS
	parent *Dir
	nodes  map[string]fs.Node
}

func (d *Dir) Attr(ctx context.Context, o *fuse.Attr) error {
	d.RLock()
	*o = d.attr
	d.RUnlock()
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.RLock()
	n, exist := d.nodes[name]
	d.RUnlock()

	if !exist {
		return nil, fuse.ENOENT
	}
	return n, nil
}

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

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
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
