package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/oort/api/proto"
	"github.com/pandemicsyn/oort/apid/flother"
	"github.com/spaolacci/murmur3"
	"golang.org/x/net/context"
)

type apiServer struct {
	sync.RWMutex
	rpool *redis.Pool
	fs    DirService
	fl    *flother.Flother
}

func (s *apiServer) GetID(custID, shareID, inode uint64) []byte {
	// TODO: Figure out what arrangement we want to use for the hash
	h := murmur3.New128()
	binary.Write(h, binary.BigEndian, custID)
	binary.Write(h, binary.BigEndian, shareID)
	binary.Write(h, binary.BigEndian, inode)
	s1, s2 := h.Sum128()
	id := make([]byte, 8)
	b := bytes.NewBuffer(id)
	binary.Write(b, binary.BigEndian, s1)
	binary.Write(b, binary.BigEndian, s2)
	return id
}

func (s *apiServer) GetAttr(ctx context.Context, r *pb.Node) (*pb.Attr, error) {
	return s.fs.GetAttr(r.Inode)
}

func (s *apiServer) SetAttr(ctx context.Context, r *pb.Attr) (*pb.Attr, error) {
	return s.fs.SetAttr(r.Inode, r)
}

func (s *apiServer) Create(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(0777),
	}
	return s.fs.Create(r.Parent, inode, r.Name, attr, false)
}

func (s *apiServer) MkDir(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
	ts := time.Now().Unix()
	inode := s.fl.GetID()
	attr := &pb.Attr{
		Inode:  inode,
		Atime:  ts,
		Mtime:  ts,
		Ctime:  ts,
		Crtime: ts,
		Mode:   uint32(os.ModeDir | 0777),
	}
	return s.fs.Create(r.Parent, inode, r.Name, attr, true)
}

func (s *apiServer) Read(ctx context.Context, r *pb.Node) (*pb.FileChunk, error) {
	var err error
	rc := s.rpool.Get()
	defer rc.Close()
	data, err := redis.Bytes(rc.Do("GET", s.GetID(1, 1, r.Inode)))
	if err != nil {
		if err == redis.ErrNil {
			//file is empty or doesn't exist yet.
			return &pb.FileChunk{}, nil
		}
		return &pb.FileChunk{}, err
	}
	f := &pb.FileChunk{Inode: r.Inode, Payload: data}
	return f, nil
}

//Write still needs to handle chunks, and validate inodes
//which means our Entry for files aslo needs to be sure to track
//blocks used in the attrs.
func (s *apiServer) Write(ctx context.Context, r *pb.FileChunk) (*pb.WriteResponse, error) {
	//sendSize := 1024 * 64
	//chunkLength := len(r.Payload)
	//start := r.Offset / sendSize
	rc := s.rpool.Get()
	defer rc.Close()
	_, err := rc.Do("SET", s.GetID(1, 1, r.Inode), r.Payload)
	if err != nil {
		return &pb.WriteResponse{Status: 1}, err
	}
	rc.Close()
	s.fs.Update(r.Inode, uint64(len(r.Payload)), time.Now().Unix())
	return &pb.WriteResponse{Status: 0}, nil
}

func (s *apiServer) Lookup(ctx context.Context, r *pb.LookupRequest) (*pb.DirEnt, error) {
	return s.fs.Lookup(r.Parent, r.Name)
}

func (s *apiServer) ReadDirAll(ctx context.Context, n *pb.Node) (*pb.DirEntries, error) {
	return s.fs.ReadDirAll(n.Inode)
}

func (s *apiServer) Remove(ctx context.Context, r *pb.DirEnt) (*pb.WriteResponse, error) {
	// TODO: Add calls to remove from backing store
	return s.fs.Remove(r.Parent, r.Name)
}
