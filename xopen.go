// Package xopen makes it easy to get buffered readers and writers.
// Ropen opens a (possibly gzipped) file/process/http site for buffered reading.
// Wopen opens a (possibly gzipped) file for buffered writing.
// Both will use gzip when appropriate and will user buffered IO.
package xopen

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"strings"

	//gzip "github.com/klauspost/pgzip"
	//"github.com/klauspost/compress/gzip"

	"compress/gzip"
)

// IsGzip returns true buffered Reader has the gzip magic.
func IsGzip(b *bufio.Reader) (bool, error) {
	return CheckBytes(b, []byte{0x1f, 0x8b})
}

// IsStdin checks if we are getting data from stdin.
func IsStdin() bool {
	// http://stackoverflow.com/a/26567513
	stat, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (stat.Mode() & os.ModeCharDevice) == 0
}

// ExpandUser expands ~/path and ~otheruser/path appropriately
func ExpandUser(path string) (string, error) {
	if path[0] != '~' {
		return path, nil
	}
	var u *user.User
	var err error
	if len(path) == 1 || path[1] == '/' {
		u, err = user.Current()
	} else {
		name := strings.Split(path[1:], "/")[0]
		u, err = user.Lookup(name)
	}
	if err != nil {
		return "", err
	}
	home := u.HomeDir
	path = home + "/" + path[1:]
	return path, nil
}

// Exists checks if a local file exits
func Exists(path string) bool {
	path, perr := ExpandUser(path)
	if perr != nil {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// CheckBytes peeks at a buffered stream and checks if the first read bytes match.
func CheckBytes(b *bufio.Reader, buf []byte) (bool, error) {

	m, err := b.Peek(len(buf))
	if err != nil {
		return false, err
	}
	for i := range buf {
		if m[i] != buf[i] {
			return false, nil
		}
	}
	return true, nil
}

// Reader is returned by Ropen
type Reader struct {
	*bufio.Reader
	rdr io.Reader
	gz  io.ReadCloser
}

// Close the associated files.
func (r *Reader) Close() error {
	if r.gz != nil {
		r.gz.Close()
	}
	if c, ok := r.rdr.(io.ReadCloser); ok {
		c.Close()
	}
	return nil
}

// Writer is returned by Wopen
type Writer struct {
	*bufio.Writer
	wtr *os.File
	gz  *gzip.Writer
}

// Close the associated files.
func (w *Writer) Close() error {
	w.Flush()
	if w.gz != nil {
		w.gz.Close()
	}
	w.wtr.Close()
	return nil
}

// Flush the writer.
func (w *Writer) Flush() {
	w.Writer.Flush()
	if w.gz != nil {
		w.gz.Flush()
	}
}

var pageSize = os.Getpagesize() * 2

// Buf returns a buffered reader from an io.Reader
// If f == "-", then it will attempt to read from os.Stdin.
// If the file is gzipped, it will be read as such.
func Buf(r io.Reader) *Reader {
	b := bufio.NewReaderSize(r, pageSize)
	var rdr io.ReadCloser
	if is, err := IsGzip(b); err != nil && err != io.EOF {
		log.Fatal(err)
	} else if is {
		rdr, err = newFastGzReader(b)
		//rdr, err = gzip.NewReader(b)
		if err != nil {
			log.Fatal(err)
		}
		b = bufio.NewReaderSize(rdr, pageSize)
	}
	return &Reader{b, r, rdr}
}

// XReader returns a reader from a url string or a file.
func XReader(f string) (io.Reader, error) {
	if strings.HasPrefix(f, "http://") || strings.HasPrefix(f, "https://") {
		var rsp *http.Response
		rsp, err := http.Get(f)
		if err != nil {
			return nil, err
		}
		if rsp.StatusCode != 200 {
			return nil, fmt.Errorf("http error downloading %s. status: %s", f, rsp.Status)
		}
		rdr := rsp.Body
		return rdr, nil
	}
	f, err := ExpandUser(f)
	if err != nil {
		return nil, err
	}
	return os.Open(f)
}

// Ropen opens a buffered reader.
func Ropen(f string) (*Reader, error) {
	var err error
	var rdr io.Reader
	if f == "-" {
		if !IsStdin() {
			return nil, errors.New("warning: stdin not detected")
		}
		b := Buf(os.Stdin)
		return b, nil
	} else if f[0] == '|' {
		// TODO: use csv to handle quoted file names.
		cmdStrs := strings.Split(f[1:], " ")
		var cmd *exec.Cmd
		if len(cmdStrs) == 2 {
			cmd = exec.Command(cmdStrs[0], cmdStrs[1:]...)
		} else {
			cmd = exec.Command(cmdStrs[0])
		}
		rdr, err = cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		err = cmd.Start()
		if err != nil {
			return nil, err
		}
	} else {
		rdr, err = XReader(f)
	}
	if err != nil {
		return nil, err
	}
	b := Buf(rdr)
	return b, nil
}

// Wopen opens a buffered reader.
// If f == "-", then stdout will be used.
// If f endswith ".gz", then the output will be gzipped.
func Wopen(f string) (*Writer, error) {
	var wtr *os.File
	var err error
	if f == "-" {
		wtr = os.Stdout
	} else {
		wtr, err = os.Create(f)
		if err != nil {
			return nil, err
		}
	}
	if !strings.HasSuffix(f, ".gz") {
		return &Writer{bufio.NewWriterSize(wtr, pageSize), wtr, nil}, nil
	}
	gz := gzip.NewWriter(wtr)
	return &Writer{bufio.NewWriterSize(gz, pageSize), wtr, gz}, nil
}
