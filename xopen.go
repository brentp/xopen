// xopen makes it easy to get buffered (possibly gzipped) readers and writers.
// and close all of the associated files.
// Ropen opens a file for reading.
// Wopen opens a file for writing.
// Both will use gzip when appropriate and will user buffered IO.
package xopen

import (
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
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
	gz  *gzip.Reader
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

// Return a buffered reader from an io.Reader
// If f == "-", then it will attempt to read from os.Stdin.
// If the file is gzipped, it will be read as such.
func Buf(r io.Reader) *Reader {
	b := bufio.NewReader(r)
	var rdr *gzip.Reader
	if is, err := IsGzip(b); err != nil {
		log.Fatal(err)
	} else if is {
		rdr, err = gzip.NewReader(b)
		if err != nil {
			log.Fatal(err)
		}
		b = bufio.NewReader(rdr)
	}
	return &Reader{b, r, rdr}
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
	} else if strings.HasPrefix(f, "http://") || strings.HasPrefix(f, "https://") {
		var rsp *http.Response
		rsp, err = http.Get(f)
		if err != nil {
			return nil, err
		}
		if rsp.StatusCode != 200 {
			return nil, fmt.Errorf("http error downloading %s. status: %s", f, rsp.Status)
		}
		rdr = rsp.Body
	} else {
		rdr, err = os.Open(f)
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
		return &Writer{bufio.NewWriter(wtr), wtr, nil}, nil
	}
	gz := gzip.NewWriter(wtr)
	return &Writer{bufio.NewWriter(gz), wtr, gz}, nil
}
