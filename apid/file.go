package main

import (
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"strconv"
	"sync"
	"time"
)

type fileServer struct {
	sync.RWMutex
	rpool *redis.Pool
	fs    *InMemFS
}

// GetAttr either here or client side should start setting the "valid" field as well.
func (s *fileServer) GetAttr(ctx context.Context, r *pb.FileRequest) (*pb.Attr, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	if entry, ok := s.fs.nodes[r.Inode]; ok {
		return entry.attr, nil
	}
	return &pb.Attr{}, nil
}

func (s *fileServer) SetAttr(ctx context.Context, r *pb.Attr) (*pb.Attr, error) {
	s.fs.Lock()
	defer s.fs.Unlock()
	if entry, ok := s.fs.nodes[r.Inode]; ok {
		entry.attr.Mode = r.Mode
		entry.attr.Size = r.Size
		entry.attr.Mtime = r.Mtime
		return entry.attr, nil
	}
	return &pb.Attr{}, nil
}

func (s *fileServer) Read(ctx context.Context, r *pb.FileRequest) (*pb.File, error) {
	var err error
	rc := s.rpool.Get()
	defer rc.Close()
	data, err := redis.Bytes(rc.Do("GET", strconv.FormatUint(r.Inode, 10)))
	if err != nil {
		if err == redis.ErrNil {
			//file is empty or doesn't exist yet.
			return &pb.File{}, nil
		}
		return &pb.File{}, err
	}
	f := &pb.File{Name: r.Fpath, Inode: r.Inode, Payload: data}
	return f, nil
}

//Write still needs to handle chunks, and validate inodes
//which means our Entry for files aslo needs to be sure to track
//blocks used in the attrs.
func (s *fileServer) Write(ctx context.Context, r *pb.File) (*pb.WriteResponse, error) {
	s.fs.Lock()
	defer s.fs.Unlock()
	rc := s.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("SET", strconv.FormatUint(r.Inode, 10), r.Payload)
	if err != nil {
		return &pb.WriteResponse{Status: 1}, err
	}
	rc.Close()
	s.fs.nodes[r.Inode].attr.Size = uint64(len(r.Payload))
	s.fs.nodes[r.Inode].attr.Mtime = time.Now().Unix()
	return &pb.WriteResponse{Status: 0}, nil
}
