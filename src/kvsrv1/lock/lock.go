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
	for {
		err := lk.ck.Put(lk.lockname, "", 0)
		// log.Printf("debug: MakeLock() gets %v", err)
		if (err == rpc.OK) || (err == rpc.ErrVersion) {
			break
		} else if err == rpc.ErrMaybe {
			continue
		}
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	myvalue := kvtest.RandValue(8)
	for {
		val, vs, err := lk.ck.Get(lk.lockname)
		// disconnect from the server or the lock is allocated
		if (err != rpc.OK) || (val != "") {
			continue
		}
		// try to get lock
		err = lk.ck.Put(lk.lockname, myvalue, vs)
		// log.Printf("debug in Acquire(): Put %s gets %v", myvalue, err)
		if err == rpc.OK {
			return
		}
		// resend request message to check if the clerk gets the lock
		if err == rpc.ErrMaybe {
			for {
				val, _, err = lk.ck.Get(lk.lockname)
				// disconnect from the server
				if err != rpc.OK {
					continue
				}
				if val == myvalue {
					// the clerk holds the lock
					return
				} else {
					// failed to get lock, try again
					break
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		_, vs, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			continue
		}
		err = lk.ck.Put(lk.lockname, "", vs)
		if err == rpc.OK || err == rpc.ErrMaybe {
			return
		}
	}

}
