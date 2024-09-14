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
	file     *bufferedReader
	buffer   []byte
	timeLine time.Duration
	timeConv time.Duration
}

func (r *reader) Next() (d linedata, err error) {
	var b byte
	start := time.Now()

	defer func(start time.Time) {
		r.timeLine += time.Since(start)
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
			convStart := time.Now()

			if d.name == "" {
				continue
			}
			// "custom" atoi
			var neg bool
			buf := r.buffer
			if buf[0] == '-' {
				neg = true
				buf = buf[1:]
			}
			switch len(buf) {
			case 3:
				d.value = int64(buf[0]-'0')*100 + int64(buf[1]-'0')*10 + int64(buf[2]-'0')
			case 2:
				d.value = int64(buf[0]-'0')*10 + int64(buf[1]-'0')
			case 1:
				d.value = int64(buf[0] - '0')
			default:
				err = fmt.Errorf("Wrong buffer len %d\"%s\"", len(buf), string(r.buffer))
			}
			if neg {
				d.value = -d.value
			}

			r.buffer = r.buffer[:0]
			convTime := time.Since(convStart)
			r.timeConv += convTime
			return
		case ' ', '\t', '.':
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
