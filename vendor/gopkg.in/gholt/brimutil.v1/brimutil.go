// Copyright Gregory Holt. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package brimutil contains tools there isn't another place for yet.
//
// This is the latest stable version of the package.
//
// For the latest development version of the package, switch to the
// master branch at https://github.com/gholt/brimutil
// or use github.com/gholt/brimutil as the import path.
//
// Also, you'd want to use http://godoc.org/github.com/gholt/brimutil
// for the development documentation.
package brimutil

import (
	"os/user"
	"path"
	"path/filepath"
	"strings"
)

// NormalizePath returns a path with any "." or ".." instances resolved
// (using path.Clean) and also attempts to resolve "~" and "~user" to home
// directories.
func NormalizePath(value string) string {
	value = path.Clean(filepath.ToSlash(value))
	if value[0] == '~' {
		if value[1] == '/' {
			if usr, err := user.Current(); err == nil {
				return strings.Replace(value, "~", usr.HomeDir, 1)
			}
		} else {
			parts := strings.SplitN(value, "/", 2)
			if usr, err := user.Lookup(parts[0][1:]); err == nil {
				return path.Join(usr.HomeDir, parts[1])
			}
		}
	}
	return value
}

// NextHighestPowerOfTwo returns the next highest power of 2 from v; assuming v
// is > 0. http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
func NextHighestPowerOfTwo(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}
