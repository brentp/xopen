[![GoDoc] (https://godoc.org/github.com/brentp/xopen?status.png)](https://godoc.org/github.com/brentp/xopen)
[![Build Status](https://travis-ci.org/brentp/xopen.svg)](https://travis-ci.org/brentp/xopen)
[![Coverage Status](https://coveralls.io/repos/brentp/xopen/badge.svg?branch=master)](https://coveralls.io/r/brentp/xopen?branch=master)

# xopen
--
    import "github.com/brentp/xopen"

xopen makes it easy to get buffered (possibly gzipped) readers and writers. and
close all of the associated files. Ropen opens a file for reading. Wopen opens a
file for writing. Both will use gzip when appropriate and will user buffered IO.

## Usage

Here's how to get a buffered reader:
```go
// gzipped
rdr, err := xopen.Ropen("some.gz")
// normal
rdr, err := xopen.Ropen("some.txt")
// stdin (possibly gzipped)
rdr, err := xopen.Ropen("-")
// https://
rdr, err := xopen.Ropen("http://example.com/some-file.txt")
// Cmd
rdr, err := xopen.Ropen("|ls -lh somefile.gz")

```
Get a buffered writer with `xopen.Wopen`.


#### func  CheckBytes

```go
func CheckBytes(b *bufio.Reader, buf []byte) (bool, error)
```
CheckBytes peeks at a buffered stream and checks if the first read bytes match.

#### func  IsGzip

```go
func IsGzip(b *bufio.Reader) (bool, error)
```
IsGzip returns true buffered Reader has the gzip magic.

#### func  IsStdin

```go
func IsStdin() bool
```
IsStdin checks if we are getting data from stdin.

#### func  XReader

```go
func XReader(f string) (io.Reader, error)
```
// XReader returns a reader from a url string or a file.


#### type Reader

```go
type Reader struct {
	*bufio.Reader
}
```

Reader is returned by Ropen

#### func  Buf

```go
func Buf(r io.Reader) *Reader
```
Return a buffered reader from an io.Reader If f == "-", then it will attempt to
read from os.Stdin. If the file is gzipped, it will be read as such.

#### func  Ropen

```go
func Ropen(f string) (*Reader, error)
```
Ropen opens a buffered reader.

#### func (*Reader) Close

```go
func (r *Reader) Close() error
```
Close the associated files.

#### type Writer

```go
type Writer struct {
	*bufio.Writer
}
```

Writer is returned by Wopen

#### func  Wopen

```go
func Wopen(f string) (*Writer, error)
```
Wopen opens a buffered reader. If f == "-", then stdout will be used. If f
endswith ".gz", then the output will be gzipped.

#### func (*Writer) Close

```go
func (w *Writer) Close() error
```
Close the associated files.

#### func (*Writer) Flush

```go
func (w *Writer) Flush()
```
Flush the writer.
