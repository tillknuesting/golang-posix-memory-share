package main

import (
	"fmt"
	"github.com/tillknuesting/litekv"
	"io/ioutil"
	"log"
	"os"
	"syscall"
)

const (
	shmName  = "/shm_example"
	lockName = "lock_example"
	shmSize  = 4096
)

func main() {
	kvs := &litekv.KeyValueStore{}

	// Write a key-value pair to the store
	kvs.Write([]byte("foo"), []byte("bar"))
	kvs.Write([]byte("foo2"), []byte("bar2"))
	kvs.Write([]byte("foo3"), []byte("bar3"))
	kvs.Write([]byte("foo4"), []byte("bar4"))

	shm, err := createShm()
	if err != nil {
		log.Fatalf("Failed to create shared memory object: %v", err)
	}
	defer cleanupShm(shm)

	lock, err := createLock()
	if err != nil {
		log.Fatalf("Failed to create lock: %v", err)
	}
	defer cleanupLock(lock)

	addr, err := mapShm(shm)
	if err != nil {
		log.Fatalf("Failed to memory map the shared memory object: %v", err)
	}
	defer unmapShm(addr)

	fmt.Println("Writing message to shared memory:", kvs.Data)
	err = writeToShm(addr, kvs.Data, lock)
	if err != nil {
		log.Fatalf("Failed to write to shared memory: %v", err)
	}

	readMsg, err := readFromShm(addr, shmSize, lock)
	if err != nil {
		log.Fatalf("Failed to read from shared memory: %v", err)
	}
	kvs2 := &litekv.KeyValueStore{Data: readMsg}
	fmt.Println("Read message from shared memory:", kvs2.Data)

	kvs2.RebuildIndex()

	v, err := kvs2.Read([]byte("foo"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo =", string(v))
	}

	v, err = kvs2.Read([]byte("foo3"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo3 =", string(v))
	}

	v, err = kvs2.Read([]byte("foo2"))
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("foo2 =", string(v))
	}
}

func createShm() (*os.File, error) {
	return os.OpenFile("/dev/shm"+shmName, os.O_CREATE|os.O_RDWR, 0666)
}

func cleanupShm(shm *os.File) {
	shm.Close()
	os.Remove("/dev/shm" + shmName)
}

func mapShm(shm *os.File) ([]byte, error) {
	err := shm.Truncate(shmSize)
	if err != nil {
		return nil, err
	}
	return syscall.Mmap(int(shm.Fd()), 0, shmSize, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}

func unmapShm(addr []byte) {
	syscall.Munmap(addr)
}

func createLock() (*os.File, error) {
	return ioutil.TempFile("/dev/shm", lockName)
}

func cleanupLock(lock *os.File) {
	lock.Close()
	os.Remove(lock.Name())
}

func lockFile(lock *os.File) error {
	return syscall.Flock(int(lock.Fd()), syscall.LOCK_EX)
}

func unlockFile(lock *os.File) error {
	return syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
}

func writeToShm(addr []byte, data []byte, lock *os.File) error {
	if len(data) > shmSize {
		return fmt.Errorf("data size exceeds shared memory size")
	}
	if err := lockFile(lock); err != nil {
		return err
	}
	defer unlockFile(lock)

	copy(addr, data)
	return nil
}

func readFromShm(addr []byte, size int, lock *os.File) ([]byte, error) {
	if size > shmSize {
		return nil, fmt.Errorf("requested size exceeds shared memory size")
	}
	if err := lockFile(lock); err != nil {
		return nil, err
	}
	defer unlockFile(lock)

	return addr[:size], nil
}
