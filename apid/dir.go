package main

import (
	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
	"sync"
)

type dirServer struct {
	sync.RWMutex
	rpool *redis.Pool
}

func (s *dirServer) Create(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}

func (s *dirServer) Lookup(ctx context.Context, f *pb.DirRequest) (*pb.DirEnt, error) {
	return &pb.DirEnt{}, nil
}

func (s *dirServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	return &pb.DirEntries{}, nil
}

func (s *dirServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	return &pb.WriteResponse{}, nil
}
