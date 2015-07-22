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
	files map[uint64]*pb.Attr //temp in memory stuff
}

func (s *fileServer) GetAttr(ctx context.Context, r *pb.FileRequest) (*pb.Attr, error) {
	s.RLock()
	defer s.RUnlock()
	if attr, ok := s.files[r.Inode]; ok {
		return attr, nil
	}
	return &pb.Attr{Name: r.Fpath}, nil
}

func (s *fileServer) SetAttr(ctx context.Context, r *pb.Attr) (*pb.Attr, error) {
	s.Lock()
	defer s.Unlock()
	f := &pb.Attr{
		Parent: "wat",
		Name:   r.Name,
		Mode:   r.Mode,
		Size:   r.Size,
		Mtime:  r.Mtime,
	}
	s.files[r.Inode] = f
	return f, nil
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

func (s *fileServer) Write(ctx context.Context, r *pb.File) (*pb.WriteResponse, error) {
	s.Lock()
	defer s.Unlock()
	rc := s.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("SET", strconv.FormatUint(r.Inode, 10), r.Payload)
	if err != nil {
		return &pb.WriteResponse{Status: 1}, err
	}
	rc.Close()
	s.files[r.Inode].Size = uint64(len(r.Payload))
	s.files[r.Inode].Mtime = time.Now().Unix()
	return &pb.WriteResponse{Status: 0}, nil
}
