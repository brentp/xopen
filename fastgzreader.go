package xopen

import (
	"compress/gzip"
	"io"
	"os/exec"
)

// from: https://gist.github.com/rasky/d42a52c16683f1a2f4dccdef80e2712d

// fastGzReader is an API-compatible drop-in replacement
// for gzip.Reader, that achieves a higher decoding speed
// by spawning an external zcat instance and pipeing data
// through it.
// Go's native gzip implementation is about 2x slower at
// decompressing data compared to zlib (mostly due to Go compiler
// inefficiencies). So for tasks where the gzip decoding
// speed is important, this is a quick workaround that doesn't
// require cgo.
// zcat is part of the gzip package and comes preinstalled on
// most Linux distributions and on OSX.
type fastGzReader struct {
	io.ReadCloser
}

func hasProg(prog string) bool {
	cmd := exec.Command(prog)
	err := cmd.Start()
	has := err == nil
	cmd.Wait()
	return has
}

var hasZcat bool = hasProg("zcat")

func newFastGzReader(r io.Reader) (io.ReadCloser, error) {

	if hasZcat {
		var gz fastGzReader
		if err := gz.Reset(r); err != nil {
			return nil, err
		}
		return gz, nil
	} else {
		return gzip.NewReader(r)
	}

}

func (gz *fastGzReader) Reset(r io.Reader) error {
	if gz.ReadCloser != nil {
		gz.Close()
	}

	cmd := exec.Command("zcat")
	cmd.Stdin = r

	rpipe, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		rpipe.Close()
		return err
	}

	gz.ReadCloser = rpipe
	return nil
}
