package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    bolt "go.etcd.io/bbolt"
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

const (
    CHECKIN_STEP = 10000
    SYNC_STEP    = 100
)

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
    reverse := false
    if reverse {
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
    } else {
        return uri
    }
}

func persist(tx *bolt.Tx, entry Entry, stats *Stats) error {
    if entry.Action == Create {
        val := fmt.Sprintf("%s!%s", entry.Rkey, entry.DID)

        key := []byte(toKey(*entry.URI))
        // fmt.Printf("key: %s\n", string(key))

        b := tx.Bucket([]byte("likes"))
        existing := b.Get(key)
        if existing == nil {
            (*stats).Subjects += 1
        } else {
            val = fmt.Sprintf("%s;%s", string(existing), val)
        }
        if err := b.Put(key, []byte(val)); err != nil {
            return err
        }

        (*stats).Likes += 1
    } else {
        key := []byte(fmt.Sprintf("unlike:%s!%s", entry.Rkey, entry.DID))
        if err := tx.Bucket([]byte("unlikes")).Put(key, []byte{}); err != nil {
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
    fmt.Printf("%d\t%d\t%.3f\n", (*stats).Entries, size, d.Seconds())
    return nil
}

func main() {
    file, err := os.Open("../likes5-simple.jsonl")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()
    scanner := bufio.NewScanner(file)

    db, err := bolt.Open("likes.bbolt", 0600, &bolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    if err := db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte("likes"))
        if err != nil {
            return err
        }
        _, err = tx.CreateBucketIfNotExists([]byte("unlikes"))
        if err != nil {
            return err
        }
        return nil
    }); err != nil {
        log.Fatal(err)
    }

    stats := Stats{}
    t0 := time.Now()

    tx, err := db.Begin(true)
    if err != nil {
        log.Fatal(err)
    }

    for scanner.Scan() {
        line := scanner.Bytes()
        entry, err := JsonToEntry(line)
        if err != nil {
            log.Fatal(err)
        }
        checkin := (stats.Entries % CHECKIN_STEP) == (CHECKIN_STEP - 1)
        sync := (stats.Entries % SYNC_STEP) == (SYNC_STEP - 1)

        if sync {
            if err := tx.Commit(); err != nil {
                log.Fatal(err)
            }
            tx, err = db.Begin(true)
            if err != nil {
                log.Fatal(err)
            }
        }

        if err := persist(tx, entry, &stats); err != nil {
            log.Fatal(err)
        }
        stats.Entries += 1

        if checkin {
            if err := showUpdate(time.Since(t0), "likes.bbolt", &stats); err != nil {
                log.Fatal(err)
            }
        }
    }
    if err := tx.Commit(); err != nil {
        log.Fatal(err)
    }

    tf := time.Now()

    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

    fmt.Printf("done in %.1fs. entries: %d, likes: %d, unlikes: %d, subjects: %d\n",
        tf.Sub(t0).Seconds(), stats.Entries, stats.Likes, stats.Unlikes, stats.Subjects)
}
