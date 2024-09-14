package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

const (
	bufferSize = 4 * 1024 * 1024

	filename  = "measurements.txt"
	filename2 = "output.csv"
)

var outFile *os.File

type linedata struct {
	name  string
	value int64
}

type data struct {
	min   int64
	max   int64
	cur   int64
	count int64
}

type bufferedReader struct {
	in io.Reader

	buf1 []byte
	buf2 []byte

	oldBuffer []byte
	buffer    []byte

	ind int
}

func NewBuffered(in io.Reader) *bufferedReader {
	b := &bufferedReader{
		in:   in,
		buf1: make([]byte, bufferSize),
		buf2: make([]byte, bufferSize),
		ind:  bufferSize,
	}

	b.oldBuffer = b.buf1
	b.buffer = b.buf2

	return b
}

func (b *bufferedReader) ReadUntil(delim byte) ([]byte, error) {
	i := b.ind

	defer func() {
		if i == len(b.buffer) {
			b.buffer = nil
		} else {
			b.buffer = b.buffer[i+1:]
		}
		b.ind = 0
	}()

	for counter := 0; counter <= 10000; counter++ {
		if i >= len(b.buffer) {
			b.buf1, b.buf2 = b.buf2, b.buf1
			b.oldBuffer = b.buffer[b.ind:]
			b.buffer = b.buf1

			n, err := b.in.Read(b.buffer)
			if err != nil {
				return nil, err
			}

			b.buffer = b.buffer[:n]
			b.ind = 0
			i = 0
		}

		if b.buffer[i] != delim {
			i++
			continue
		}

		if len(b.oldBuffer) > 0 {
			res := append(b.oldBuffer, b.buffer[:i]...)
			b.oldBuffer = nil
			return res, nil
		}

		return b.buffer[:i], nil
	}

	return nil, fmt.Errorf("Zhopa")
}

type reader struct {
	file     *bufferedReader
	buffer   []byte
	timeLine time.Duration
	timeConv time.Duration
}

func (r *reader) Next() (d linedata, err error) {
	var (
		buf []byte
		neg bool
	)

	start := time.Now()

	buf, err = r.file.ReadUntil(';')
	if err != nil {
		return
	}

	i := 0
	for buf[i] == ' ' || buf[i] == '\t' {
		i++
	}
	d.name = string(buf[i])

	buf, err = r.file.ReadUntil('\n')
	if err != nil {
		return
	}

	conv := time.Now()
	// "atoi"
	if buf[0] == '-' {
		neg = true
		buf = buf[1:]
	}

	switch len(buf) {
	case 4:
		d.value = int64(buf[0]-'0')*100 + int64(buf[1]-'0')*10 + int64(buf[3]-'0')
	case 3:
		d.value = int64(buf[0]-'0')*10 + int64(buf[2]-'0')
	default:
		err = fmt.Errorf("Wrong buffer len %d\"%s\"", len(buf), string(r.buffer))
	}

	if neg {
		d.value = -d.value
	}

	r.timeLine += conv.Sub(start)
	r.timeConv += time.Since(conv)

	return
}

type calculator struct {
	r    *reader
	data map[string]data

	timeAssign time.Duration
	timeCalc   time.Duration
}

func (c *calculator) Proccess() error {
	c.data = map[string]data{}
	for {
		line, err := c.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		start := time.Now()

		val := c.data[line.name]
		val.cur += line.value
		val.min = min(val.min, line.value)
		val.max = max(val.max, line.value)
		val.count++
		c.data[line.name] = val

		c.timeAssign += time.Since(start)
	}

	start := time.Now()

	for k, v := range c.data {
		avg := v.cur / v.count
		fmt.Fprintf(outFile, "%s;%d.%d;%d.%d;%d.%d\n", k,
			v.min/10, abs(v.min)%10,
			avg/10, abs(avg)%10,
			v.max/10, abs(v.max)%10)
	}

	c.timeCalc += time.Since(start)

	return nil
}

func abs(a int64) int64 {
	if a < 0 {
		return -a
	}
	return a
}

func main() {
	file, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	outFile, err = os.Create(filename2)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	r := &reader{file: NewBuffered(file)}
	r.buffer = make([]byte, 0, 100)
	calc := &calculator{r: r}
	err = calc.Proccess()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	fmt.Printf("read = %s\nconv = %s\nassign = %s\ncalc+output = %s\n", r.timeLine-r.timeConv, r.timeConv, calc.timeAssign, calc.timeCalc)
}
