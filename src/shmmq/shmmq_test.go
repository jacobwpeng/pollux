package shmmq_test

import (
	"bytes"
	_ "fmt"
	"math/rand"
	_ "os"
	"shmmq"
	_ "syscall"
	"testing"
)

//func TestPush(t *testing.T) {
//	filePath := "/tmp/mq"
//	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
//	if err != nil {
//		t.Errorf("open %s failed: %q", filePath, err)
//	}
//
//	fileInfo, err := file.Stat()
//	if err != nil {
//		t.Errorf("stat file failed: %q", err)
//	}
//
//	mem, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()),
//		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
//
//	if err != nil {
//		t.Errorf("mmap failed: %q", err)
//	}
//
//	mq, err := shmmq.Restore(mem)
//	if err != nil {
//		t.Errorf("restore shmmq failed: %q", err)
//	}
//	for i := 0; i < 10000000; {
//		upperLimit := mq.SpaceLeft()
//		if upperLimit > shmmq.MaxDataSize {
//			upperLimit = shmmq.MaxDataSize
//		}
//		if upperLimit == 0 {
//			continue
//		}
//
//		sz := rand.Intn(int(upperLimit))
//		if sz == 0 {
//			continue
//		}
//		data := make([]byte, sz)
//		ok := mq.Push(data)
//		if ok {
//			i += 1
//			if i%10000 == 0 {
//				fmt.Printf("Push data size: %d, i: %d\n", sz, i)
//			}
//		}
//	}
//}

//func TestPop(t *testing.T) {
//	filePath := "/tmp/mq"
//	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
//	if err != nil {
//		t.Errorf("open %s failed: %q", filePath, err)
//	}
//
//	fileInfo, err := file.Stat()
//	if err != nil {
//		t.Errorf("stat file failed: %q", err)
//	}
//
//	mem, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()),
//		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
//
//	if err != nil {
//		t.Errorf("mmap failed: %q", err)
//	}
//
//	mq, err := shmmq.Restore(mem)
//	if err != nil {
//		t.Errorf("restore shmmq failed: %q", err)
//	}
//	for i := 0; i < 10000000; {
//		data := mq.Pop()
//		if data != nil {
//			i += 1
//			if i%10000 == 0 {
//				fmt.Printf("i: %d\n", i)
//			}
//		}
//	}
//}

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
	data := make([]byte, shmmq.MaxDataSize)
	go func() {
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
			ok := writeMQ.Push(data[:sz])
			if !ok {
				panic("Push failed")
			}
			c <- data[:sz]
			i += 1
		}
	}()

	for i := 0; i < dataNum; {
		dataFromChan := <-c
		var data []byte
		for data == nil {
			data = readMQ.Pop()
		}
		if !bytes.Equal(data, dataFromChan) {
			t.Errorf("data not match")
		}
		i += 1
	}
}
