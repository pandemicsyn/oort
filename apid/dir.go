package main

import (
	"bazil.org/fuse/fs"
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"

	"sync"
	"time"
)

type dirServer struct {
	sync.RWMutex
	rpool *redis.Pool
	dirs  map[string]Dir
}

type Dir struct {
	sync.RWMutex
	attr   *pb.FileAttr
	parent string
	nodes  map[string]fs.Node
}

func (s *dirServer) GetAttr(ctx context.Context, r *pb.DirRequest) (*pb.FileAttr, error) {
	s.RLock()
	defer s.RUnlock()
	if dentry, ok := s.dirs[r.Name]; ok {
		return dentry.attr, nil
	}
	return &pb.FileAttr{Name: r.Name}, nil
}

func (s *dirServer) Create(ctx context.Context, r *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}

func (s *dirServer) Lookup(ctx context.Context, r *pb.DirRequest) (*pb.DirEnt, error) {
	s.RLock()
	defer s.RUnlock()
	_, exists := s.dirs[r.Name]
	if !exists {
		return &pb.DirEnt{}, nil
	}
	// fake for now
	ts := time.Now().Unix()
	dattr := &pb.DirAttr{
		Name:   r.Name,
		Inode:  uint64(time.Now().UnixNano()),
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   1777,
		Valid:  5,
	}
	return &pb.DirEnt{Name: r.Name, Attr: dattr}, nil
}

func (s *dirServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	return &pb.DirEntries{}, nil
}

func (s *dirServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}
