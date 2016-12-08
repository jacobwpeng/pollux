package shmmq_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"pollux/shmmq"
	"testing"
)

func TestLockFree(t *testing.T) {
	bufferSize := shmmq.MinMemSize * 1000
	mem := make([]byte, bufferSize)

	readMQ, err := shmmq.Create(mem)
	if err != nil {
		t.Errorf("create shmmq failed: %q", err)
	}

	writeMQ, err := shmmq.Restore(mem)
	if err != nil {
		t.Errorf("restore shmmq failed: %q", err)
	}

	const dataNum int = 100000
	c := make(chan []byte, dataNum)
	go func() {
		data := make([]byte, shmmq.MaxDataSize)
		for i := 0; i < dataNum; {
			upperLimit := writeMQ.SpaceLeft()
			if upperLimit == 0 {
				continue
			}
			if upperLimit >= shmmq.MaxDataSize {
				upperLimit = shmmq.MaxDataSize
			}
			sz := rand.Intn(int(upperLimit))
			if sz == 0 {
				continue
			}
			_, err := writeMQ.Write(data[:sz])
			if err != nil {
				panic("Write failed")
			}
			c <- data[:sz]
			i += 1
		}
	}()

	data := make([]byte, shmmq.MaxDataSize)
	for i := 0; i < dataNum; {
		dataFromChan := <-c
		n := 0
		for {
			n, err = readMQ.Read(data)
			if err == nil {
				break
			} else {
				fmt.Println(err)
			}
		}
		if !bytes.Equal(data[:n], dataFromChan) {
			t.Errorf("data not match")
		}
		i += 1
		if i%10000 == 0 {
			fmt.Printf("Receive %d messages\n", i)
		}
	}
}
