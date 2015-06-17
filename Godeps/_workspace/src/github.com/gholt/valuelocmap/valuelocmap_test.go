package valuelocmap

import "testing"

func TestSetNewKeyOldTimestampIs0AndNewKeySaved(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp := uint64(2)
	blockID := uint32(1)
	offset := uint32(0)
	length := uint32(0)
	oldTimestamp := vlm.Set(keyA, keyB, timestamp, blockID, offset, length, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp {
		t.Fatal(timestampGet, timestamp)
	}
	if blockIDGet != blockID {
		t.Fatal(blockIDGet, blockID)
	}
	if offsetGet != offset {
		t.Fatal(offsetGet, offset)
	}
	if lengthGet != length {
		t.Fatal(lengthGet, length)
	}
}

func TestSetOverwriteKeyOldTimestampIsOldAndOverwriteWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 + 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetOldOverwriteKeyOldTimestampIsPreviousAndPreviousWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(4)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 - 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyOldTimestampIsSameAndOverwriteIgnored(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyOldTimestampIsSameAndOverwriteWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, true)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetOverflowingKeys(t *testing.T) {
	vlm := New(OptRoots(1), OptPageSize(1)).(*valueLocMap)
	keyA1 := uint64(0)
	keyB1 := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	oldTimestamp := vlm.Set(keyA1, keyB1, timestamp1, blockID1, offset1, length1, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA1, keyB1)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
	keyA2 := uint64(0)
	keyB2 := uint64(2)
	timestamp2 := timestamp1 + 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp = vlm.Set(keyA2, keyB2, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet = vlm.Get(keyA2, keyB2)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
}

func TestSetOverflowingKeysReuse(t *testing.T) {
	vlm := New(OptRoots(1), OptPageSize(1)).(*valueLocMap)
	keyA1 := uint64(0)
	keyB1 := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	oldTimestamp := vlm.Set(keyA1, keyB1, timestamp1, blockID1, offset1, length1, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA1, keyB1)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
	keyA2 := uint64(0)
	keyB2 := uint64(2)
	timestamp2 := timestamp1 + 2
	blockID2 := blockID1 + 1
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp = vlm.Set(keyA2, keyB2, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet = vlm.Get(keyA2, keyB2)
	if timestampGet != timestamp2 {
		t.Fatal(timestampGet, timestamp2)
	}
	if blockIDGet != blockID2 {
		t.Fatal(blockIDGet, blockID2)
	}
	if offsetGet != offset2 {
		t.Fatal(offsetGet, offset2)
	}
	if lengthGet != length2 {
		t.Fatal(lengthGet, length2)
	}
	oldTimestamp = vlm.Set(keyA2, keyB2, timestamp2, uint32(0), offset2, length2, true)
	if oldTimestamp != timestamp2 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet = vlm.Get(keyA2, keyB2)
	if timestampGet != 0 {
		t.Fatal(timestampGet)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet)
	}
	keyA3 := uint64(0)
	keyB3 := uint64(2)
	timestamp3 := timestamp1 + 4
	blockID3 := blockID1 + 2
	offset3 := offset1 + 2
	length3 := length1 + 2
	oldTimestamp = vlm.Set(keyA3, keyB3, timestamp3, blockID3, offset3, length3, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet = vlm.Get(keyA3, keyB3)
	if timestampGet != timestamp3 {
		t.Fatal(timestampGet, timestamp3)
	}
	if blockIDGet != blockID3 {
		t.Fatal(blockIDGet, blockID3)
	}
	if offsetGet != offset3 {
		t.Fatal(offsetGet, offset3)
	}
	if lengthGet != length3 {
		t.Fatal(lengthGet, length3)
	}
	if vlm.roots[0].used != 2 {
		t.Fatal(vlm.roots[0].used)
	}
}

func TestSetOverflowingKeysLots(t *testing.T) {
	vlm := New(OptRoots(1), OptPageSize(1), OptSplitMultiplier(1000)).(*valueLocMap)
	keyA := uint64(0)
	timestamp := uint64(2)
	blockID := uint32(1)
	offset := uint32(2)
	length := uint32(3)
	for keyB := uint64(0); keyB < 100; keyB++ {
		vlm.Set(keyA, keyB, timestamp, blockID, offset, length, false)
		blockID++
		offset++
		length++
	}
	if vlm.roots[0].used != 100 {
		t.Fatal(vlm.roots[0].used)
	}
	if len(vlm.roots[0].overflow) != 25 {
		t.Fatal(len(vlm.roots[0].overflow))
	}
	blockID = uint32(1)
	offset = uint32(2)
	length = uint32(3)
	for keyB := uint64(0); keyB < 100; keyB++ {
		timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
		if timestampGet != timestamp {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, timestamp)
		}
		if blockIDGet != blockID {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, blockID)
		}
		if offsetGet != offset {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, offset)
		}
		if lengthGet != length {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, length)
		}
		blockID++
		offset++
		length++
	}
	timestamp2 := timestamp + 2
	blockID = uint32(2)
	offset = uint32(3)
	length = uint32(4)
	for keyB := uint64(0); keyB < 75; keyB++ {
		timestampSet := vlm.Set(keyA, keyB, timestamp2, blockID, offset, length, false)
		if timestampSet != timestamp {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampSet, timestamp)
		}
		blockID++
		offset++
		length++
	}
	blockID = uint32(2)
	offset = uint32(3)
	length = uint32(4)
	for keyB := uint64(0); keyB < 75; keyB++ {
		timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
		if timestampGet != timestamp2 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, timestamp2)
		}
		if blockIDGet != blockID {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, blockID)
		}
		if offsetGet != offset {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, offset)
		}
		if lengthGet != length {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, length)
		}
		blockID++
		offset++
		length++
	}
	if vlm.roots[0].used != 100 {
		t.Fatal(vlm.roots[0].used)
	}
	timestamp3 := timestamp2 + 2
	for keyB := uint64(0); keyB < 50; keyB++ {
		timestampSet := vlm.Set(keyA, keyB, timestamp3, uint32(0), uint32(0), uint32(0), false)
		if timestampSet != timestamp2 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampSet, timestamp2)
		}
		blockID++
		offset++
		length++
	}
	blockID = uint32(2)
	offset = uint32(3)
	length = uint32(4)
	for keyB := uint64(0); keyB < 50; keyB++ {
		timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
		if timestampGet != 0 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, 0)
		}
		if blockIDGet != 0 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, 0)
		}
		if offsetGet != 0 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, 0)
		}
		if lengthGet != 0 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, 0)
		}
		blockID++
		offset++
		length++
	}
	timestamp4 := timestamp3 + 2
	blockID = uint32(7)
	offset = uint32(8)
	length = uint32(9)
	for keyB := uint64(200); keyB < 300; keyB++ {
		vlm.Set(keyA, keyB, timestamp4, blockID, offset, length, false)
		blockID++
		offset++
		length++
	}
	if vlm.roots[0].used != 150 {
		t.Fatal(vlm.roots[0].used)
	}
	blockID = uint32(1)
	offset = uint32(2)
	length = uint32(3)
	for keyB := uint64(0); keyB < 100; keyB++ {
		timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
		if keyB < 50 {
			if timestampGet != 0 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, 0)
			}
			if blockIDGet != 0 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, 0)
			}
			if offsetGet != 0 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, 0)
			}
			if lengthGet != 0 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, 0)
			}
		} else if keyB < 75 {
			if timestampGet != timestamp2 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, timestamp2)
			}
			if blockIDGet != blockID+1 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, blockID+1)
			}
			if offsetGet != offset+1 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, offset+1)
			}
			if lengthGet != length+1 {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, length+1)
			}
		} else {
			if timestampGet != timestamp {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, timestamp)
			}
			if blockIDGet != blockID {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, blockID)
			}
			if offsetGet != offset {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, offset)
			}
			if lengthGet != length {
				t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, length)
			}
		}
		blockID++
		offset++
		length++
	}
	blockID = uint32(7)
	offset = uint32(8)
	length = uint32(9)
	for keyB := uint64(200); keyB < 300; keyB++ {
		timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
		if timestampGet != timestamp4 {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, timestampGet, timestamp4)
		}
		if blockIDGet != blockID {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, blockIDGet, blockID)
		}
		if offsetGet != offset {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, offsetGet, offset)
		}
		if lengthGet != length {
			t.Fatalf("%016x %016x %d %d", keyA, keyB, lengthGet, length)
		}
		blockID++
		offset++
		length++
	}
}

func TestSetNewKeyBlockID0OldTimestampIs0AndNoEffect(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp := uint64(2)
	blockID := uint32(0)
	offset := uint32(4)
	length := uint32(5)
	oldTimestamp := vlm.Set(keyA, keyB, timestamp, blockID, offset, length, false)
	if oldTimestamp != 0 {
		t.Fatal(oldTimestamp)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsOldAndOverwriteWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 + 2
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}

func TestSetOldOverwriteKeyBlockID0OldTimestampIsPreviousAndPreviousWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(4)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1 - 2
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsSameAndOverwriteIgnored(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, false)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != timestamp1 {
		t.Fatal(timestampGet, timestamp1)
	}
	if blockIDGet != blockID1 {
		t.Fatal(blockIDGet, blockID1)
	}
	if offsetGet != offset1 {
		t.Fatal(offsetGet, offset1)
	}
	if lengthGet != length1 {
		t.Fatal(lengthGet, length1)
	}
}

func TestSetOverwriteKeyBlockID0OldTimestampIsSameAndOverwriteWins(t *testing.T) {
	vlm := New().(*valueLocMap)
	keyA := uint64(0)
	keyB := uint64(0)
	timestamp1 := uint64(2)
	blockID1 := uint32(1)
	offset1 := uint32(0)
	length1 := uint32(0)
	vlm.Set(keyA, keyB, timestamp1, blockID1, offset1, length1, false)
	timestamp2 := timestamp1
	blockID2 := uint32(0)
	offset2 := offset1 + 1
	length2 := length1 + 1
	oldTimestamp := vlm.Set(keyA, keyB, timestamp2, blockID2, offset2, length2, true)
	if oldTimestamp != timestamp1 {
		t.Fatal(oldTimestamp, timestamp1)
	}
	timestampGet, blockIDGet, offsetGet, lengthGet := vlm.Get(keyA, keyB)
	if timestampGet != 0 {
		t.Fatal(timestampGet, 0)
	}
	if blockIDGet != 0 {
		t.Fatal(blockIDGet, 0)
	}
	if offsetGet != 0 {
		t.Fatal(offsetGet, 0)
	}
	if lengthGet != 0 {
		t.Fatal(lengthGet, 0)
	}
}
