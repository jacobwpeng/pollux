package shmmq

import (
	"encoding/binary"
	"fmt"
	"os"
	_ "sync/atomic"
	"syscall"
	"unsafe"
)

const MaxDataSize uint64 = 65536
const headerSize uint64 = 16
const dataPrefixSize uint64 = 4
const extraSpaceSize uint64 = 1
const MinMemSize uint64 = headerSize + extraSpaceSize + MaxDataSize

type MessageQueue struct {
	file *os.File
	mem  []byte
}

func CreateFromFile(file *os.File, offset int64,
	size int64) (*MessageQueue, error) {

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("Stat file error: %s", err)
	}

	regionSize := fileInfo.Size() - offset
	if regionSize <= 0 {
		return nil, fmt.Errorf("Invalid mapped region size: %d", regionSize)
	}

	autoCloseFile := true
	defer func() {
		if autoCloseFile {
			file.Close()
		}
	}()

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED
	mem, err := syscall.Mmap(int(file.Fd()), offset, int(regionSize), prot, flags)
	if err != nil {
		return nil, fmt.Errorf("Mmap file error: %s", err)
	}

	mq, err := Create(mem)
	if err != nil {
		return nil, err
	}

	mq.file = file
	autoCloseFile = false
	return mq, nil
}

func Create(mem []byte) (*MessageQueue, error) {
	memSize := uint64(len(mem))
	if memSize < MinMemSize {
		return nil, fmt.Errorf("mem size too small, min required: %d", MinMemSize)
	}
	mq := &MessageQueue{mem: mem}
	mq.setReadIndex(0)
	mq.setWriteIndex(0)
	return mq, nil
}

func Restore(mem []byte) (*MessageQueue, error) {
	memSize := uint64(len(mem))
	if memSize < MinMemSize {
		return nil, fmt.Errorf("mem size too small, min required: %d", MinMemSize)
	}

	var mq MessageQueue
	mq.mem = mem
	readIndex := mq.readIndex()
	writeIndex := mq.writeIndex()

	dataRegionSize := memSize - headerSize
	if readIndex > dataRegionSize {
		return nil, fmt.Errorf("invalid read index: %d, data region size: %d",
			readIndex, dataRegionSize)
	}

	if writeIndex > dataRegionSize {
		return nil, fmt.Errorf("invalid write index: %d, data region size: %d",
			writeIndex, dataRegionSize)
	}

	return &mq, nil
}

func (mq *MessageQueue) Read(p []byte) (n int, err error) {
	dataSize, dataOffset := mq.readDataSize()
	if int(dataSize) > len(p) {
		return 0, fmt.Errorf("Buffer too small, expect: %d, actual: %d",
			dataSize, len(p))
	}
	if dataSize == 0 {
		return 0, fmt.Errorf("No data to read")
	}
	memSize := uint64(len(mq.mem))

	if dataOffset+dataSize <= memSize {
		copy(p, mq.mem[dataOffset:])
		mq.setReadIndex(dataOffset + dataSize - headerSize)
		return int(dataSize), nil
	}

	firstPart := memSize - dataOffset
	copy(p, mq.mem[dataOffset:])
	copy(p[firstPart:], mq.mem[headerSize:])
	mq.setReadIndex(dataSize - firstPart)
	return int(dataSize), nil
}

func (mq *MessageQueue) Write(p []byte) (n int, err error) {
	dataSize := uint64(len(p))

	if dataSize == 0 {
		return 0, fmt.Errorf("Zero length data not supported")
	}

	if dataSize > MaxDataSize {
		return 0, fmt.Errorf("Data too large, max: %d", MaxDataSize)
	}

	spaceLeft := mq.SpaceLeft()
	if dataSize > spaceLeft {
		return 0, fmt.Errorf("No space for data, space: %d", spaceLeft)
	}

	offset := mq.writeDataSize(dataSize)
	offset = mq.writeData(offset, p)
	mq.setWriteIndex(offset - headerSize)
	return len(p), nil
}

//func (mq *MessageQueue) Push(data []byte) bool {
//	dataSize := uint64(len(data))
//
//	if dataSize > MaxDataSize {
//		return false
//	}
//	if dataSize > mq.SpaceLeft() {
//		return false
//	}
//
//	offset := mq.writeDataSize(dataSize)
//	offset = mq.writeData(offset, data)
//	mq.setWriteIndex(offset - headerSize)
//	return true
//}
//
//func (mq *MessageQueue) Pop() []byte {
//	dataSize, dataOffset := mq.readDataSize()
//	if dataSize == 0 {
//		return nil
//	}
//	data, offset := mq.readData(uint64(dataSize), dataOffset)
//	mq.setReadIndex(offset - headerSize)
//	return data
//}

func (mq *MessageQueue) Empty() bool {
	return mq.readIndex() == mq.writeIndex()
}

func (mq *MessageQueue) readIndex() uint64 {
	readIndexPtr := (*uint64)(unsafe.Pointer(&mq.mem[0]))
	return *readIndexPtr
	//return atomic.LoadUint64(readIndexPtr)
}

func (mq *MessageQueue) setReadIndex(val uint64) {
	readIndexPtr := (*uint64)(unsafe.Pointer(&mq.mem[0]))
	*readIndexPtr = val
	//atomic.StoreUint64(readIndexPtr, val)
}

func (mq *MessageQueue) writeIndex() uint64 {
	writeIndexPtr := (*uint64)(unsafe.Pointer(&mq.mem[8]))
	return *writeIndexPtr
	//return atomic.LoadUint64(writeIndexPtr)
}

func (mq *MessageQueue) setWriteIndex(val uint64) {
	writeIndexPtr := (*uint64)(unsafe.Pointer(&mq.mem[8]))
	*writeIndexPtr = val
	//atomic.StoreUint64(writeIndexPtr, val)
}

func (mq *MessageQueue) SpaceLeft() uint64 {
	readIndex := mq.readIndex()
	writeIndex := mq.writeIndex()

	memSize := uint64(len(mq.mem))
	const minSize uint64 = dataPrefixSize + extraSpaceSize
	var spaceLeft uint64
	if writeIndex < readIndex {
		spaceLeft = readIndex - writeIndex
	} else {
		spaceLeft = memSize - writeIndex + readIndex - headerSize
	}

	if spaceLeft > minSize {
		return spaceLeft - minSize
	} else {
		return 0
	}
}

func (mq *MessageQueue) writeDataSize(sz uint64) uint64 {
	if sz > MaxDataSize {
		panic(fmt.Sprintf("Invalid data size: ", sz))
	}
	sizeBuf := make([]byte, dataPrefixSize)
	binary.LittleEndian.PutUint32(sizeBuf, uint32(sz))
	memSize := uint64(len(mq.mem))
	offset := headerSize + mq.writeIndex()

	if offset+dataPrefixSize <= memSize {
		copy(mq.mem[offset:], sizeBuf)
		offset += dataPrefixSize
		return offset
	}

	firstPart := memSize - offset
	copy(mq.mem[offset:], sizeBuf[:])
	copy(mq.mem[headerSize:], sizeBuf[firstPart:])
	offset = headerSize + dataPrefixSize - firstPart

	return offset
}

func (mq *MessageQueue) writeData(offset uint64, data []byte) uint64 {
	dataSize := uint64(len(data))
	if dataSize > MaxDataSize {
		panic(fmt.Sprintf("Invalid data size: ", dataSize))
	}
	memSize := uint64(len(mq.mem))

	if offset+dataSize <= memSize {
		copy(mq.mem[offset:], data)
		offset += dataSize
		return offset
	}

	firstPart := memSize - offset
	copy(mq.mem[offset:], data[:firstPart])
	copy(mq.mem[headerSize:], data[firstPart:])
	offset = headerSize + dataSize - firstPart
	return offset
}

func (mq *MessageQueue) readDataSize() (uint64, uint64) {
	readIndex := mq.readIndex()
	writeIndex := mq.writeIndex()
	memSize := uint64(len(mq.mem))

	if readIndex == writeIndex {
		return 0, readIndex + headerSize
	}

	offset := readIndex + headerSize
	dataOffset := offset + dataPrefixSize
	var dataSize uint32
	if dataOffset <= memSize {
		dataSize = binary.LittleEndian.Uint32(mq.mem[offset:dataOffset])
	} else {
		buf := make([]byte, dataPrefixSize)
		firstPart := memSize - offset
		copy(buf[:firstPart], mq.mem[offset:])
		copy(buf[firstPart:], mq.mem[headerSize:])
		dataSize = binary.LittleEndian.Uint32(buf)
		dataOffset = headerSize + dataPrefixSize - firstPart
	}
	return uint64(dataSize), dataOffset
}

func (mq *MessageQueue) readData(dataSize uint64,
	dataOffset uint64) ([]byte, uint64) {
	if dataSize == 0 || dataSize > MaxDataSize {
		panic(fmt.Sprintf("Invalid data size: %d", dataSize))
	}

	data := make([]byte, dataSize)
	memSize := uint64(len(mq.mem))

	if dataOffset+dataSize <= memSize {
		copy(data, mq.mem[dataOffset:])
		return data, dataOffset + dataSize
	}

	firstPart := memSize - dataOffset
	copy(data, mq.mem[dataOffset:])
	copy(data[firstPart:], mq.mem[headerSize:])
	return data, headerSize + dataSize - firstPart
}
