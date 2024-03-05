package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func main() {
	path := os.Args[1]
	b, err := lsmkv.NewBucket(context.Background(), path, "", &logrus.Logger{}, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop())
	if err != nil {
		fmt.Println("create objects bucket")
		return
	}

	cursor := b.Cursor()
	var updates []struct {
		oldKey []byte
		newKey []byte
		value  []byte
	}

	bigEndian := 0
	littleEndian := 0
	maxExpected := uint64(10_000_000_000)

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		oldKey := make([]byte, 8)
		copy(oldKey, k)

		id := binary.BigEndian.Uint64(k)

		if id == 0 {
			continue
		}

		if id < maxExpected {
			bigEndian++

			littleEndianBytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(littleEndianBytes, id)

			updates = append(updates, struct {
				oldKey []byte
				newKey []byte
				value  []byte
			}{oldKey, littleEndianBytes, v})

		} else {
			littleEndian++
		}
	}

	cursor.Close()

	for _, update := range updates {
		err = b.Delete(update.oldKey)
		if err != nil {
			fmt.Printf("Error deleting key: %s\n", err)
			return
		}
		err = b.Put(update.newKey, update.value)
		if err != nil {
			fmt.Printf("Error putting key: %s\n", err)
			return
		}
	}

	b.WriteWAL()
	b.Shutdown(context.Background())

	fmt.Println(bigEndian, littleEndian)
}
