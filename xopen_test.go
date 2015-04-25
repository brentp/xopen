package xopen

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	. "gopkg.in/check.v1"
	"io"
	"os"
	"strings"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type SniffTest struct{}

var _ = Suite(&SniffTest{})

func gzFromString(s string) string {
	var c bytes.Buffer
	gz := gzip.NewWriter(&c)
	gz.Write([]byte(s))
	return c.String()
}

var gzTests = []struct {
	isGz bool
	data string
}{
	{false, "asdf"},
	{true, gzFromString("asdf")},
}

func (s *SniffTest) TestIsGzip(c *C) {
	for _, t := range gzTests {
		isGz, err := IsGzip(bufio.NewReader(strings.NewReader(t.data)))
		c.Assert(err, IsNil)
		c.Assert(t.isGz, Equals, isGz)
	}
}

func (s *SniffTest) TestIsStdin(c *C) {
	r := IsStdin()
	c.Assert(r, Equals, false)
}

func (s *SniffTest) TestRopen(c *C) {
	rdr, err := Ropen("-")
	c.Assert(err, ErrorMatches, ".* stdin not detected")
	c.Assert(rdr, IsNil)
}

func (s *SniffTest) TestWopen(c *C) {
	testString := "ASDF1234"
	wtr, err := Wopen("t.gz")
	c.Assert(err, IsNil)
	_, err = os.Stat("t.gz")
	c.Assert(err, IsNil)
	c.Assert(wtr.wtr, NotNil)
	fmt.Fprintf(wtr, testString)
	wtr.Close()

	rdr, err := Ropen("t.gz")
	c.Assert(err, IsNil)

	str, err := rdr.ReadString(99)
	c.Assert(str, Equals, testString)
	c.Assert(err, Equals, io.EOF)
	str, err = rdr.ReadString(99)
	c.Assert(str, Equals, "")

	rdr.Close()
	os.Remove("t.gz")
}
