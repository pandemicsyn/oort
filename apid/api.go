package main

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	pb "github.com/pandemicsyn/ort/api/proto"
	"golang.org/x/net/context"
)

type apiServer struct {
	sync.RWMutex
	rpool *redis.Pool
	fs    *InMemFS
}

func (s *apiServer) GetAttr(ctx context.Context, r *pb.FileRequest) (*pb.Attr, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	if entry, ok := s.fs.nodes[r.Inode]; ok {
		return entry.attr, nil
	}
	return &pb.Attr{}, nil
}

func (s *apiServer) Create(ctx context.Context, r *pb.FileEnt) (*pb.FileEnt, error) {
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

func (s *apiServer) Read(ctx context.Context, r *pb.FileRequest) (*pb.File, error) {
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
func (s *apiServer) Write(ctx context.Context, r *pb.File) (*pb.WriteResponse, error) {
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

func (s *apiServer) MkDir(ctx context.Context, r *pb.DirEnt) (*pb.DirEnt, error) {
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

func (s *apiServer) Lookup(ctx context.Context, r *pb.LookupRequest) (*pb.DirEnt, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	inode, exists := s.fs.nodes[r.Parent].entries[r.Name]
	if !exists {
		return &pb.DirEnt{}, nil
	}
	entry := s.fs.nodes[inode]
	return &pb.DirEnt{Name: entry.path, Attr: entry.attr}, nil
}

func (s *apiServer) ReadDirAll(ctx context.Context, f *pb.DirRequest) (*pb.DirEntries, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	e := &pb.DirEntries{}
	for i, _ := range s.fs.nodes[f.Inode].ientries {
		entry := s.fs.nodes[i]
		if entry.isdir {
			e.DirEntries = append(e.DirEntries, &pb.DirEnt{Name: entry.path, Attr: entry.attr})
		} else {
			e.FileEntries = append(e.FileEntries, &pb.FileEnt{Name: entry.path, Attr: entry.attr})
		}
	}
	return e, nil
}

func (s *apiServer) Remove(ctx context.Context, f *pb.DirEnt) (*pb.WriteResponse, error) {
	s.fs.RLock()
	defer s.fs.RUnlock()
	return &pb.WriteResponse{}, nil
}
