package main

import (
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type dirServer struct {
	sync.RWMutex
	rpool *redis.Pool
	fs    *InMemFS
}

func (s *dirServer) GetAttr(ctx context.Context, r *pb.DirRequest) (*pb.Attr, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	if entry, ok := s.fs.nodes[r.Inode]; ok {
		return entry.attr, nil
	}
	return &pb.Attr{Name: r.Name, Inode: r.Inode}, nil
}

func (s *dirServer) Create(ctx context.Context, r *pb.FileEnt) (*pb.FileEnt, error) {
	s.fs.Lock()
	defer s.fs.Unlock()
	if _, exists := s.fs.nodes[r.Parent].entries[r.Name]; exists {
		return &pb.FileEnt{}, nil
	}
	n := &Entry{
		path:     r.Name,
		UUIDNode: time.Now().UnixNano(),
		isdir:    false,
		entries:  make(map[string]uint64),
		ientries: make(map[uint64]string),
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
	s.fs.nodes[n.attr.Inode] = n
	s.fs.nodes[r.Parent].entries[r.Name] = n.attr.Inode
	s.fs.nodes[r.Parent].ientries[n.attr.Inode] = r.Name
	atomic.AddUint64(&s.fs.nodes[r.Parent].nodeCount, 1)
	return &pb.FileEnt{Name: n.path, Attr: n.attr}, nil
}

func (s *dirServer) MkDir(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	s.fs.Lock()
	defer s.fs.Unlock()
	if _, exists := s.fs.nodes[r.Parent].entries[r.Name]; exists {
		return &pb.DirEnt{}, nil
	}
	n := &Entry{
		path:     r.Name,
		UUIDNode: time.Now().UnixNano(),
		isdir:    true,
		entries:  make(map[string]uint64),
		ientries: make(map[uint64]string),
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
	s.fs.nodes[n.attr.Inode] = n
	s.fs.nodes[r.Parent].entries[r.Name] = n.attr.Inode
	s.fs.nodes[r.Parent].ientries[n.attr.Inode] = r.Name
	atomic.AddUint64(&s.fs.nodes[r.Parent].nodeCount, 1)
	return &pb.DirEnt{Name: n.path, Attr: n.attr}, nil
}

func (s *dirServer) Lookup(ctx context.Context, r *pb.LookupRequest) (*pb.DirEnt, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	inode, exists := s.fs.nodes[r.Parent].entries[r.Name]
	if !exists {
		return &pb.DirEnt{}, nil
	}
	entry := s.fs.nodes[inode]
	return &pb.DirEnt{Name: entry.path, Attr: entry.attr}, nil
}

func (s *dirServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	return &pb.DirEntries{}, nil
}

func (s *dirServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	return &pb.WriteResponse{}, nil
}
