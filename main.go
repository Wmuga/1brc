package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

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

type reader struct {
	file        *bufio.Reader
	timeLine    time.Duration
	timeConvert time.Duration
}

func (r *reader) Next() (d linedata, err error) {
	var line string

	start := time.Now()
	for line == "" {
		line, err = r.file.ReadString('\n')
		if err != nil {
			return d, err
		}
		line = strings.TrimSpace(line)
	}

	split := time.Now()

	splitData := strings.Split(line, ";")
	d.name = splitData[0]
	d.value, err = strconv.ParseFloat(splitData[1], 64)

	end := time.Now()

	r.timeLine += split.Sub(start)
	r.timeConvert += end.Sub(split)

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

	r := &reader{file: bufio.NewReader(file)}
	calc := &calculator{r: r}
	calc.Proccess()

	fmt.Printf("read = %s\nnconvert = %s\nassign = %s\ncalc = %s\n", r.timeLine, r.timeConvert, calc.timeAssign, calc.timeAssign)
}
