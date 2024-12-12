package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/cockroachdb/pebble"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Action int

const (
	Create Action = iota
	Delete
)

const CHECKIN_STEP = 10000

type Entry struct {
	Action Action
	DID    string
	Rkey   string
	URI    *string
}

type Stats struct {
	Entries  uint64
	Likes    uint64
	Unlikes  uint64
	Subjects uint64
}

func JsonToEntry(line []byte) (Entry, error) {
	entry := Entry{}
	var rawEntry []*string
	if err := json.Unmarshal(line, &rawEntry); err != nil {
		return entry, err
	}
	if len(rawEntry) != 4 {
		return entry, fmt.Errorf("wrong format for entry, expected 4 elements, found %d", len(rawEntry))
	}
	if rawEntry[0] == nil {
		return entry, fmt.Errorf("action was nil")
	}
	if *rawEntry[0] == "c" {
		entry.Action = Create
	} else if *rawEntry[0] == "d" {
		entry.Action = Delete
	} else {
		return entry, fmt.Errorf("action was not 'c' or 'd': '%s'", *rawEntry[0])
	}

	if rawEntry[1] == nil {
		return entry, fmt.Errorf("did was nil")
	}
	entry.DID = *rawEntry[1]

	if rawEntry[2] == nil {
		return entry, fmt.Errorf("rkey was nil")
	}
	entry.Rkey = *rawEntry[2]

	entry.URI = rawEntry[3]

	return entry, nil
}

func toKey(uri string) string {
	at, rest, found := strings.Cut(uri, "://")
	if !found || at != "at" {
		return uri
	}
	did, rest, found := strings.Cut(rest, "/")
	if !found {
		return uri
	}
	collection, rkey, found := strings.Cut(rest, "/")
	if !found {
		return uri
	}

	return fmt.Sprintf("%s\\%s\\%s", collection, rkey, did)
}

func persist(db *pebble.DB, entry Entry, writeOpts *pebble.WriteOptions, stats *Stats) error {
	if entry.Action == Create {
		val := fmt.Sprintf("%s!%s", entry.Rkey, entry.DID)

		key := []byte(fmt.Sprintf("like:%s", toKey(*entry.URI)))
		// fmt.Printf("key: %s\n", string(key))

		existing, closer, err := db.Get(key)
		if err == pebble.ErrNotFound {
			(*stats).Subjects += 1
		} else if err != nil {
			return err
		} else {
			val = fmt.Sprintf("%s;%s", existing, val)
			if err := closer.Close(); err != nil {
				return err
			}
		}
		if err := db.Set(key, []byte(val), writeOpts); err != nil {
			return err
		}
		(*stats).Likes += 1
	} else {
		key := []byte(fmt.Sprintf("unlike:%s!%s", entry.Rkey, entry.DID))
		if err := db.Set(key, []byte(""), pebble.Sync); err != nil {
			return err
		}
		(*stats).Unlikes += 1
	}
	return nil
}

func DirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func showUpdate(d time.Duration, dbPath string, stats *Stats) error {
	size, err := DirSize(dbPath)
	if err != nil {
		return nil // skip update if dirsize fails
	}
	fmt.Printf("%d\t%d\t%.1f\n", (*stats).Entries, size, d.Seconds())
	return nil
}

func main() {
	file, err := os.Open("../likes5-simple.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)

	db, err := pebble.Open("likes.pebble", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}

	stats := Stats{}
	t0 := time.Now()

	for scanner.Scan() {
		line := scanner.Bytes()
		entry, err := JsonToEntry(line)
		if err != nil {
			log.Fatal(err)
		}
		checkin := (stats.Entries % CHECKIN_STEP) == (CHECKIN_STEP - 1)

		writeOpts := pebble.NoSync
		if checkin {
			writeOpts = pebble.Sync
		}

		if err := persist(db, entry, writeOpts, &stats); err != nil {
			log.Fatal(err)
		}
		stats.Entries += 1

		if checkin {
			if err := showUpdate(time.Since(t0), "likes.pebble", &stats); err != nil {
				log.Fatal(err)
			}
		}
	}

	tf := time.Now()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("done in %.1fs. entries: %d, likes: %d, unlikes: %d, subjects: %d\n",
		tf.Sub(t0).Seconds(), stats.Entries, stats.Likes, stats.Unlikes, stats.Subjects)
}
