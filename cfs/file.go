package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/pandemicsyn/ort/api/proto"
)

type File struct {
	sync.RWMutex
	attr fuse.Attr
	path string
	fs   *CFS
	data []byte
}

// TODO: Fix race (I rlock but then modify attrs).
// Probably need to acquire lock on the api server.
func (f *File) Attr(ctx context.Context, o *fuse.Attr) error {
	f.RLock()
	grpclog.Printf("Getting attrs for %s", f.path)

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	a, err := f.fs.fc.GetAttr(rctx, &pb.FileRequest{Fpath: f.path})
	if err != nil {
		grpclog.Fatalf("%v.GetAttr(_) = _, %v: ", f.fs.fc, err)
	}
	f.attr.Mode = os.FileMode(a.Mode)
	f.attr.Size = a.Size
	f.attr.Mtime = time.Unix(a.Mtime, 0)
	*o = f.attr
	f.RUnlock()
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle,
	error) {
	return f, nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	f.RLock()
	out := make([]byte, len(f.data))
	grpclog.Printf("Getting attrs for %s", f.path)
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	rf, err := f.fs.fc.Read(rctx, &pb.FileRequest{Fpath: f.path})
	if err != nil {
		grpclog.Fatalf("%v.GetAttr(_) = _, %v: ", f.fs.fc, err)
	}
	copy(out, rf.Payload)
	f.RUnlock()
	return out, nil
}

// Write only works with tiny writes right now and doesn't write/append chunks to the backend at all!
// We also write all data to memory AND the current chunk to the backend.
// So, the whole thing only sorta kinda works.
// TODO: Write chunks
// TODO: Update backend attrs (size!)
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.Lock()
	l := len(req.Data)
	end := int(req.Offset) + l
	if end > len(f.data) {
		delta := end - len(f.data)
		f.data = append(f.data, make([]byte, delta)...)
		f.attr.Size = uint64(len(f.data))
		atomic.AddInt64(&f.fs.size, int64(delta))
		grpclog.Printf("Updating attrs for %s", f.path)
		a := &pb.FileAttr{
			Parent: "something",
			Name:   f.path,
			Mode:   uint32(f.attr.Mode),
			Size:   f.attr.Size,
			Mtime:  f.attr.Mtime.Unix(),
		}
		rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		rf, err := f.fs.fc.SetAttr(rctx, a)
		if err != nil {
			grpclog.Fatalf("%v.SetAttr(_) = _, %v: ", f.fs.fc, err)
		}
		grpclog.Printf("%v, Updated attrs: %+v", f.path, rf)
	}
	copy(f.data[req.Offset:end], req.Data)
	grpclog.Printf("Writing to backend for %s", f.path)
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	rf, err := f.fs.fc.Write(rctx, &pb.File{Name: f.path, Payload: f.data})
	if err != nil {
		grpclog.Fatalf("%v.Write(_) = _, %v: ", f.fs.fc, err)
	}
	if rf.Status != 0 {
		grpclog.Println("Write status non zero")
	}
	copy(f.data[req.Offset:end], req.Data)
	resp.Size = l
	f.Unlock()
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest,
	resp *fuse.SetattrResponse) error {
	f.Lock()

	if req.Valid.Size() {
		delta := int(req.Size) - len(f.data)
		if delta > 0 {
			f.data = append(f.data, make([]byte, delta)...)
		} else {
			f.data = f.data[0:req.Size]
		}
		f.attr.Size = req.Size
		atomic.AddInt64(&f.fs.size, int64(delta))
	}

	if req.Valid.Mode() {
		f.attr.Mode = req.Mode
	}

	if req.Valid.Atime() {
		f.attr.Atime = req.Atime
	}

	if req.Valid.AtimeNow() {
		f.attr.Atime = time.Now()
	}

	if req.Valid.Mtime() {
		f.attr.Mtime = req.Mtime
	}

	if req.Valid.MtimeNow() {
		f.attr.Mtime = time.Now()
	}

	resp.Attr = f.attr
	grpclog.Printf("Writing attrs for %s", f.path)
	a := &pb.FileAttr{
		Parent: "something",
		Name:   f.path,
		Mode:   uint32(f.attr.Mode),
		Size:   f.attr.Size,
		Mtime:  f.attr.Mtime.Unix(),
	}
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	rf, err := f.fs.fc.SetAttr(rctx, a)
	if err != nil {
		grpclog.Fatalf("%v.SetAttr(_) = _, %v: ", f.fs.fc, err)
	}
	grpclog.Printf("%v, Updated attrs: %+v", f.path, rf)
	f.Unlock()
	return nil
}
