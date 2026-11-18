package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	// You may add code here
	lk.ck.Put(lk.lockname, "0", 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, vs, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK && val == "0" {
			err = lk.ck.Put(lk.lockname, "1", vs)
			if err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, vs, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK && val == "1" {
			err = lk.ck.Put(lk.lockname, "0", vs)
			if err == rpc.OK {
				return
			}
		}
	}

}
