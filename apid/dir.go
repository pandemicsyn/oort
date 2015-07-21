package main

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"bazil.org/fuse/fs"
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
)

type dirServer struct {
	sync.RWMutex
	rpool     *redis.Pool
	nodes     map[string]*Entry
	nodeCount uint64
}

type Entry struct {
	path  string
	isdir bool
	sync.RWMutex
	attr     *pb.Attr
	parent   string
	nodes    map[string]fs.Node
	UUIDNode int64
}

func (s *dirServer) GetAttr(ctx context.Context, r *pb.DirRequest) (*pb.Attr, error) {
	s.RLock()
	defer s.RUnlock()
	if entry, ok := s.nodes[r.Name]; ok {
		return entry.attr, nil
	}
	return &pb.Attr{Name: r.Name}, nil
}

func (s *dirServer) Create(ctx context.Context, r *pb.FileEnt) (*pb.FileEnt, error) {
	if _, exists := s.nodes[r.Name]; exists {
		return &pb.FileEnt{}, nil
	}
	n := &Entry{
		path:     r.Name,
		UUIDNode: time.Now().UnixNano(),
	}
	ts := time.Now().Unix()
	n.attr = &pb.Attr{
		Inode:  uint64(n.UUIDNode),
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(0777),
		Name:   r.Name,
	}
	s.nodes[r.Name] = n
	atomic.AddUint64(&s.nodeCount, 1)
	return &pb.FileEnt{Name: n.path, Attr: n.attr}, nil
}

func (s *dirServer) MkDir(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	if _, exists := s.nodes[r.Name]; exists {
		return &pb.DirEnt{}, nil
	}
	n := &Entry{
		path:     r.Name,
		UUIDNode: time.Now().UnixNano(),
	}
	ts := time.Now().Unix()
	n.attr = &pb.Attr{
		Inode:  uint64(n.UUIDNode),
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir | 0777),
		Name:   r.Name,
	}
	s.nodes[r.Name] = n
	atomic.AddUint64(&s.nodeCount, 1)
	return &pb.DirEnt{Name: n.path, Attr: n.attr}, nil
}

func (s *dirServer) Lookup(ctx context.Context, r *pb.DirRequest) (*pb.DirEnt, error) {
	s.RLock()
	defer s.RUnlock()
	entry, exists := s.nodes[r.Name]
	if !exists {
		return &pb.DirEnt{}, nil
	}
	return &pb.DirEnt{Name: r.Name, Attr: entry.attr}, nil
}

func (s *dirServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	return &pb.DirEntries{}, nil
}

func (s *dirServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}
