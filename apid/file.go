package main

import (
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"sync"
)

type fileServer struct {
	sync.RWMutex
	rpool *redis.Pool
	files map[string]*pb.FileAttr //temp in memory stuff
}

func (s *fileServer) GetAttr(ctx context.Context, r *pb.FileRequest) (*pb.FileAttr, error) {
	s.RLock()
	defer s.RUnlock()
	if attr, ok := s.files[r.Fpath]; ok {
		return attr, nil
	}
	return &pb.FileAttr{Name: r.Fpath}, nil
}

func (s *fileServer) SetAttr(ctx context.Context, r *pb.FileAttr) (*pb.FileAttr, error) {
	s.Lock()
	defer s.Unlock()
	f := &pb.FileAttr{
		Parent: "wat",
		Name:   r.Name,
		Mode:   r.Mode,
		Size:   r.Size,
		Mtime:  r.Mtime,
	}
	s.files[r.Name] = f
	return f, nil
}

func (s *fileServer) Read(ctx context.Context, r *pb.FileRequest) (*pb.File, error) {
	var err error
	rc := s.rpool.Get()
	defer rc.Close()
	data, err := redis.Bytes(rc.Do("GET", r.Fpath))
	if err != nil {
		if err == redis.ErrNil {
			//file is empty or doesn't exist yet.
			return &pb.File{}, nil
		}
		return &pb.File{}, err
	}
	f := &pb.File{Name: r.Fpath, Payload: data}
	return f, nil
}

func (s *fileServer) Write(ctx context.Context, r *pb.File) (*pb.WriteResponse, error) {
	rc := s.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("SET", r.Name, r.Payload)
	if err != nil {
		return &pb.WriteResponse{Status: 1}, err
	}
	rc.Close()
	return &pb.WriteResponse{Status: 0}, nil
}
