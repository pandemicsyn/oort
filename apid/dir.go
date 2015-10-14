package main

import (
	"sync"
	"sync/atomic"

	pb "github.com/pandemicsyn/ort/api/proto"
)

type DirService interface {
	GetAttr(inode uint64) (*pb.Attr, error)
	SetAttr(inode uint64, attr *pb.Attr) (*pb.Attr, error)
	Create(parent, inode uint64, name string, attr *pb.Attr, isdir bool) (*pb.DirEnt, error)
	Update(inode, size uint64, mtime int64)
	Lookup(parent uint64, name string) (*pb.DirEnt, error)
	ReadDirAll(inode uint64) (*pb.DirEntries, error)
	Remove(parent uint64, name string) (*pb.WriteResponse, error)
}

// In memory implementation of DirService
type InMemFS struct {
	sync.RWMutex
	nodes map[uint64]*Entry
}

// Entry describes each node in our fs.
// it also contains a list of all other entries "in this node".
// i.e. all files/directory in this directory.
type Entry struct {
	path  string // string path/name for this entry
	isdir bool
	sync.RWMutex
	attr      *pb.Attr
	parent    uint64            // inode of the parent
	inode     uint64            //the original/actual inode incase fuse stomps on the one in attr
	entries   map[string]uint64 // subdir/files by name
	ientries  map[uint64]string // subdir/files by inode
	nodeCount uint64            // uint64
}

func (fs *InMemFS) GetAttr(inode uint64) (*pb.Attr, error) {
	fs.RLock()
	defer fs.RUnlock()
	if entry, ok := fs.nodes[inode]; ok {
		return entry.attr, nil
	}
	return &pb.Attr{}, nil
}

func (fs *InMemFS) SetAttr(inode uint64, attr *pb.Attr) (*pb.Attr, error) {
	fs.Lock()
	defer fs.Unlock()
	if entry, ok := fs.nodes[inode]; ok {
		entry.attr.Mode = attr.Mode
		entry.attr.Size = attr.Size
		entry.attr.Mtime = attr.Mtime
		return entry.attr, nil
	}
	return &pb.Attr{}, nil
}

func (fs *InMemFS) Create(parent, inode uint64, name string, attr *pb.Attr, isdir bool) (*pb.DirEnt, error) {
	fs.Lock()
	defer fs.Unlock()
	if _, exists := fs.nodes[parent].entries[name]; exists {
		return &pb.DirEnt{}, nil
	}
	entry := &Entry{
		path:  name,
		inode: inode,
		isdir: isdir,
		attr:  attr,
	}
	if isdir {
		entry.entries = make(map[string]uint64)
		entry.ientries = make(map[uint64]string)
	}
	fs.nodes[inode] = entry
	fs.nodes[parent].entries[name] = inode
	fs.nodes[parent].ientries[inode] = name
	atomic.AddUint64(&fs.nodes[parent].nodeCount, 1)
	return &pb.DirEnt{Name: name, Attr: attr}, nil
}

func (fs *InMemFS) Lookup(parent uint64, name string) (*pb.DirEnt, error) {
	fs.RLock()
	defer fs.RUnlock()
	inode, ok := fs.nodes[parent].entries[name]
	if !ok {
		return &pb.DirEnt{}, nil
	}
	entry := fs.nodes[inode]
	return &pb.DirEnt{Name: entry.path, Attr: entry.attr}, nil
}

func (fs *InMemFS) ReadDirAll(inode uint64) (*pb.DirEntries, error) {
	fs.RLock()
	defer fs.RUnlock()
	e := &pb.DirEntries{}
	for i, _ := range fs.nodes[inode].ientries {
		entry := fs.nodes[i]
		if entry.isdir {
			e.DirEntries = append(e.DirEntries, &pb.DirEnt{Name: entry.path, Attr: entry.attr})
		} else {
			e.FileEntries = append(e.FileEntries, &pb.DirEnt{Name: entry.path, Attr: entry.attr})
		}
	}
	return e, nil
}

func (fs *InMemFS) Remove(parent uint64, name string) (*pb.WriteResponse, error) {
	fs.Lock()
	defer fs.Unlock()
	inode, ok := fs.nodes[parent].entries[name]
	if !ok {
		return &pb.WriteResponse{Status: 1}, nil
	}
	delete(fs.nodes, inode)
	delete(fs.nodes[parent].entries, name)
	delete(fs.nodes[parent].ientries, inode)
	atomic.AddUint64(&fs.nodes[parent].nodeCount, ^uint64(0)) // -1
	return &pb.WriteResponse{Status: 0}, nil
}

func (fs *InMemFS) Update(inode, size uint64, mtime int64) {
	// NOTE: Not sure what this function really should look like yet
	fs.nodes[inode].attr.Size = size
	fs.nodes[inode].attr.Mtime = mtime
}
