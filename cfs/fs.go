package main

import (
	"log"
	"os"
	"time"

	"golang.org/x/net/context"

	pb "github.com/pandemicsyn/ort/api/proto"

	"bazil.org/fuse"
	"bazil.org/fuse/fuseutil"
)

type fs struct {
	conn *fuse.Conn
	rpc  *rpc
}

func newfs(c *fuse.Conn, r *rpc) *fs {
	fs := &fs{
		conn: c,
		rpc:  r,
	}
	return fs
}

// Handle fuse request
func (f *fs) handle(r fuse.Request) {
	switch r := r.(type) {
	default:
		log.Printf("Unhandled request: %v", r)
		r.RespondError(fuse.ENOSYS)

	case *fuse.GetattrRequest:
		f.handleGetattr(r)

	case *fuse.LookupRequest:
		f.handleLookup(r)

	case *fuse.MkdirRequest:
		f.handleMkdir(r)

	case *fuse.OpenRequest:
		f.handleOpen(r)

	case *fuse.ReadRequest:
		f.handleRead(r)

	case *fuse.WriteRequest:
		f.handleWrite(r)

	case *fuse.CreateRequest:
		f.handleCreate(r)

	case *fuse.SetattrRequest:
		f.handleSetattr(r)

	case *fuse.ReleaseRequest:
		f.handleRelease(r)

	case *fuse.FlushRequest:
		f.handleFlush(r)
		/*
			case *fuse.MknodRequest:
				f.handleMknod(r)

			case *fuse.InitRequest:
				f.handleInit(r)

			case *fuse.StatfsRequest:
				f.handleStatfs(r)

			case *fuse.SetattrRequest:
				f.handleSetattr(r)

			case *fuse.SymlinkRequest:
				f.handleSymlink(r)

			case *fuse.ReadlinkRequest:
				f.handleReadlink(r)

			case *fuse.LinkRequest:
				f.handleLink(r)

			case *fuse.RemoveRequest:
				f.handleRemove(r)

			case *fuse.AccessRequest:
				f.handleAccess(r)

			case *fuse.GetxattrRequest:
				f.handleGetxattr(r)

			case *fuse.ListxattrRequest:
				f.handleListxattr(r)

			case *fuse.SetxattrRequest:
				f.handleSetxattr(r)

			case *fuse.RemovexattrRequest:
				f.handleRemovexattr(r)

			case *fuse.ForgetRequest:
				f.handleForget(r)

			case *fuse.DestroyRequest:
				f.handleDestroy(r)

			case *fuse.RenameRequest:
				f.handleRename(r)

			case *fuse.FsyncRequest:
				f.handleFsync(r)

			case *fuse.InterruptRequest:
				f.handleInterrupt(r)
		*/
	}
}

// Note: All handle functions should call r.Respond or r.Respond error before returning

func (f *fs) handleGetattr(r *fuse.GetattrRequest) {
	log.Println("Inside handleGetattr")
	log.Println(r)
	resp := &fuse.GetattrResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	a, err := f.rpc.api.GetAttr(rctx, &pb.FileRequest{Inode: uint64(r.Node)})
	if err != nil {
		log.Fatalf("GetAttr fail: %v", err)
	}
	resp.Attr.Mode = os.FileMode(a.Mode)
	resp.Attr.Size = a.Size
	resp.Attr.Mtime = time.Unix(a.Mtime, 0)

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleLookup(r *fuse.LookupRequest) {
	log.Println("Inside handleLookup")
	log.Printf("Running Lookup for %s", r.Name)
	log.Println(r)
	resp := &fuse.LookupResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	l, err := f.rpc.api.Lookup(rctx, &pb.LookupRequest{Name: r.Name, Parent: uint64(r.Node)})

	if err != nil {
		log.Fatalf("Lookup failed(%s): %v", r.Name, err)
	}
	// If there is no name then it wasn't found
	if l.Name != r.Name {
		log.Printf("ENOENT Lookup(%s)", r.Name)
		r.RespondError(fuse.ENOENT)
		return
	}
	resp.Node = fuse.NodeID(l.Attr.Inode)
	resp.Attr.Inode = l.Attr.Inode
	resp.Attr.Mode = os.FileMode(l.Attr.Mode)
	resp.Attr.Size = l.Attr.Size
	resp.Attr.Mtime = time.Unix(l.Attr.Mtime, 0)
	resp.Attr.Atime = time.Unix(l.Attr.Atime, 0)
	resp.Attr.Ctime = time.Unix(l.Attr.Ctime, 0)
	resp.Attr.Crtime = time.Unix(l.Attr.Crtime, 0)
	resp.EntryValid = 5 * time.Second

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleMkdir(r *fuse.MkdirRequest) {
	log.Println("Inside handleMkdir")
	log.Println(r)
	resp := &fuse.MkdirResponse{}

	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	m, err := f.rpc.api.MkDir(rctx, &pb.DirEnt{Name: r.Name, Parent: uint64(r.Node)})
	if err != nil {
		log.Fatalf("Mkdir failed(%s): %v", r.Name, err)
	}
	// If the name is empty, then the dir already exists
	if m.Name != r.Name {
		log.Printf("EEXIST Mkdir(%s)", r.Name)
		r.RespondError(fuse.EEXIST)
		return
	}
	resp.Node = fuse.NodeID(m.Attr.Inode)
	resp.Attr.Inode = m.Attr.Inode
	resp.Attr.Mode = os.FileMode(m.Attr.Mode)
	resp.Attr.Size = m.Attr.Size
	resp.Attr.Mtime = time.Unix(m.Attr.Mtime, 0)
	resp.Attr.Atime = time.Unix(m.Attr.Atime, 0)
	resp.Attr.Ctime = time.Unix(m.Attr.Ctime, 0)
	resp.Attr.Crtime = time.Unix(m.Attr.Crtime, 0)
	resp.EntryValid = 5 * time.Second

	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleOpen(r *fuse.OpenRequest) {
	log.Println("Inside handleOpen")
	log.Println(r)
	resp := &fuse.OpenResponse{}
	// TODO: Figure out what to do for file handles
	// For now use the inode as the file handle
	resp.Handle = fuse.HandleID(r.Node)
	log.Println(resp)
	r.Respond(resp)
}

func (f *fs) handleRead(r *fuse.ReadRequest) {
	log.Println("Inside handleRead")
	log.Println(r)
	resp := &fuse.ReadResponse{Data: make([]byte, 0, r.Size)}
	if r.Dir {
		// handle directory listing
		rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

		d, err := f.rpc.api.ReadDirAll(rctx, &pb.FileRequest{Inode: uint64(r.Node)})
		if err != nil {
			log.Fatalf("Read on dir failed: %v", err)
		}
		log.Println(d.DirEntries)
		var data []byte
		data = fuse.AppendDirent(data, fuse.Dirent{
			Name:  ".",
			Inode: uint64(r.Node),
			Type:  fuse.DT_Dir,
		})
		data = fuse.AppendDirent(data, fuse.Dirent{
			Name: "..",
			Type: fuse.DT_Dir,
		})
		for _, de := range d.DirEntries {
			log.Println(de)
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  de.Name,
				Inode: de.Attr.Inode,
				Type:  fuse.DT_Dir,
			})
		}
		for _, fe := range d.FileEntries {
			log.Println(fe)
			data = fuse.AppendDirent(data, fuse.Dirent{
				Name:  fe.Name,
				Inode: fe.Attr.Inode,
				Type:  fuse.DT_File,
			})
		}
		fuseutil.HandleRead(r, resp, data)
		r.Respond(resp)
		return
	} else {
		// handle file read
		rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
		data, err := f.rpc.api.Read(rctx, &pb.FileRequest{Inode: uint64(r.Node)})
		if err != nil {
			log.Fatal("Read on file failed: ", err)
		}
		fuseutil.HandleRead(r, resp, data.Payload)
		r.Respond(resp)
	}
}

func (f *fs) handleWrite(r *fuse.WriteRequest) {
	log.Println("Inside handleWrite")
	// TODO: Implement write
	// Currently this is stupid simple and doesn't handle all the possibilities
	if r.Offset > 0 {
		// Writing offsets isn't supported yet
		log.Printf("Writing offsets not supported yet (%v)", r.Offset)
		r.RespondError(fuse.ENOSYS)
		return
	}

	resp := &fuse.WriteResponse{}
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	w, err := f.rpc.api.Write(rctx, &pb.File{Inode: uint64(r.Node), Payload: r.Data})
	if err != nil {
		log.Fatalf("Write to file failed: %v", err)
	}
	if w.Status != 0 {
		log.Printf("Write status non zero(%d)\n", w.Status)
	}
	resp.Size = len(r.Data)
	r.Respond(resp)
}

func (f *fs) handleCreate(r *fuse.CreateRequest) {
	log.Println("Inside handleCreate")

	resp := &fuse.CreateResponse{}
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	c, err := f.rpc.api.Create(rctx, &pb.FileEnt{Parent: uint64(r.Node), Name: r.Name})
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	resp.Node = fuse.NodeID(c.Attr.Inode)
	resp.Attr.Inode = c.Attr.Inode
	resp.Attr.Mode = os.FileMode(c.Attr.Mode)
	resp.Attr.Atime = time.Unix(c.Attr.Atime, 0)
	resp.Attr.Mtime = time.Unix(c.Attr.Mtime, 0)
	resp.Attr.Ctime = time.Unix(c.Attr.Ctime, 0)
	resp.Attr.Crtime = time.Unix(c.Attr.Crtime, 0)
	resp.EntryValid = 5 * time.Second
	resp.LookupResponse.Node = fuse.NodeID(c.Attr.Inode)
	resp.LookupResponse.Attr.Inode = c.Attr.Inode
	resp.LookupResponse.Attr.Mode = os.FileMode(c.Attr.Mode)
	resp.LookupResponse.Attr.Atime = time.Unix(c.Attr.Atime, 0)
	resp.LookupResponse.Attr.Mtime = time.Unix(c.Attr.Mtime, 0)
	resp.LookupResponse.Attr.Ctime = time.Unix(c.Attr.Ctime, 0)
	resp.LookupResponse.Attr.Crtime = time.Unix(c.Attr.Crtime, 0)
	resp.LookupResponse.EntryValid = 5 * time.Second
	r.Respond(resp)
}

func (f *fs) handleSetattr(r *fuse.SetattrRequest) {
	log.Println("Inside handleSetattr")
	log.Println(r)
	resp := &fuse.SetattrResponse{}

	// Todo: Need to read attrs in to update
	if r.Valid.Size() {
		resp.Attr.Size = r.Size
	}
	if r.Valid.Mode() {
		resp.Attr.Mode = r.Mode
	}
	if r.Valid.Atime() {
		resp.Attr.Atime = r.Atime
	}
	if r.Valid.AtimeNow() {
		resp.Attr.Atime = time.Now()
	}
	if r.Valid.Mtime() {
		resp.Attr.Mtime = r.Mtime
	}

	a := &pb.Attr{
		Parent: "ishouldreallytracktheparent",
		Mode:   uint32(r.Mode),
		Size:   r.Size,
		Mtime:  r.Mtime.Unix(),
	}
	rctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := f.rpc.api.SetAttr(rctx, a)
	if err != nil {
		log.Fatalf("Setattr failed: %v", err)
	}
	r.Respond(resp)
}

func (f *fs) handleFlush(r *fuse.FlushRequest) {
	log.Println("Inside handleFlush")
	r.Respond()
}

func (f *fs) handleRelease(r *fuse.ReleaseRequest) {
	log.Println("Inside handleRelease")
	r.Respond()
}

// TODO: Implement the following functions (and make sure to comment out the case)

func (f *fs) handleMknod(r *fuse.MknodRequest) {
	log.Println("Inside handleMknod")
	// NOTE: We probably will not need this since we implement Create
	r.RespondError(fuse.EIO)
}

func (f *fs) handleInit(r *fuse.InitRequest) {
	log.Println("Inside handleInit")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleStatfs(r *fuse.StatfsRequest) {
	log.Println("Inside handleStatfs")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleSymlink(r *fuse.SymlinkRequest) {
	log.Println("Inside handleSymlink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleReadlink(r *fuse.ReadlinkRequest) {
	log.Println("Inside handleReadlink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleLink(r *fuse.LinkRequest) {
	log.Println("Inside handleLink")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleAccess(r *fuse.AccessRequest) {
	log.Println("Inside handleAccess")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRemove(r *fuse.RemoveRequest) {
	log.Println("Inside handleRemove")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleGetxattr(r *fuse.GetxattrRequest) {
	log.Println("Inside handleGetxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleListxattr(r *fuse.ListxattrRequest) {
	log.Println("Inside handleListxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleSetxattr(r *fuse.SetxattrRequest) {
	log.Println("Inside handleSetxattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRemovexattr(r *fuse.RemovexattrRequest) {
	log.Println("Inside handleRemovexattr")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleForget(r *fuse.ForgetRequest) {
	log.Println("Inside handleForget")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleDestroy(r *fuse.DestroyRequest) {
	log.Println("Inside handleDestroy")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleRename(r *fuse.RenameRequest) {
	log.Println("Inside handleRename")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleFsync(r *fuse.FsyncRequest) {
	log.Println("Inside handleFsync")
	r.RespondError(fuse.ENOSYS)
}

func (f *fs) handleInterrupt(r *fuse.InterruptRequest) {
	log.Println("Inside handleInterrupt")
	r.RespondError(fuse.ENOSYS)
}
