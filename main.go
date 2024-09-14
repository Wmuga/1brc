package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

const bufferSize = 4 * 1024 * 1024

type linedata struct {
	name  string
	value float64
}

type data struct {
	min   float64
	max   float64
	avg   float64
	cur   float64
	count int64
}

type bufferedReader struct {
	in     io.Reader
	buffer []byte
	ind    int
	max    int
}

func NewBuffered(in io.Reader) *bufferedReader {
	return &bufferedReader{
		in:     in,
		buffer: make([]byte, bufferSize),
		ind:    bufferSize,
		max:    bufferSize,
	}
}

func (b *bufferedReader) ReadByte() (byte, error) {
	if b.ind == b.max {
		n, err := b.in.Read(b.buffer)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
		b.max = n
		b.ind = 0
	}
	b.ind++
	return b.buffer[b.ind-1], nil
}

type reader struct {
	file         *bufferedReader
	buffer       []byte
	timeLineConv time.Duration
}

func (r *reader) Next() (d linedata, err error) {
	var b byte
	start := time.Now()
	defer func(start time.Time) {
		r.timeLineConv += time.Since(start)
	}(start)

	for i := 0; i < 10000; i++ {
		b, err = r.file.ReadByte()
		if err != nil {
			return
		}

		switch b {
		case ';':
			d.name = string(r.buffer)
			r.buffer = r.buffer[:0]
			continue
		case '\n':
			if d.name == "" {
				continue
			}
			d.value, err = strconv.ParseFloat(string(r.buffer), 64)
			r.buffer = r.buffer[:0]
			return
		case ' ', '\t':
			continue
		default:
			r.buffer = append(r.buffer, b)
		}
	}
	fmt.Println("Zhopa")
	os.Exit(1)
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
		v.avg = v.cur / float64(v.count)
		c.data[k] = v
	}

	c.timeCalc += time.Since(start)

	return nil
}

const filename = "measurements.txt"

func main() {
	file, err := os.Open(filename)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer file.Close()

	r := &reader{file: NewBuffered(file)}
	r.buffer = make([]byte, 0, 100)
	calc := &calculator{r: r}
	err = calc.Proccess()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	fmt.Printf("read+convert = %s\nassign = %s\ncalc = %s\n", r.timeLineConv, calc.timeAssign, calc.timeCalc)
}
