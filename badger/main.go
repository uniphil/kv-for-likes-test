package main

import (
    "bufio"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "strings"
    "time"
    badger "github.com/dgraph-io/badger/v4"
)

type Action int

const (
    Create Action = iota
    Delete
)

const CHECKIN_STEP = 10000
const SYNC_STEP = 100

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

// func addValue(originalValue, newValue []byte) []byte {
//     if len(originalValue) == 0 {
//         return newValue
//     }
//     return append(originalValue, byte(";"), newValue)
// }

func persist(db *badger.DB, entry Entry, stats *Stats) error {
    if entry.Action == Create {
        val := fmt.Sprintf("%s!%s", entry.Rkey, entry.DID)

        key := []byte(fmt.Sprintf("like:%s", toKey(*entry.URI)))
        // fmt.Printf("key: %s\n", string(key))

        if err := db.Update(func(txn *badger.Txn) error {
            item, err := txn.Get(key)
            if err == badger.ErrKeyNotFound {
                (*stats).Subjects += 1
            } else if err != nil {
                return err
            } else {
                if err := item.Value(func(existing []byte) error {
                    val = fmt.Sprintf("%s;%s", string(existing), val)
                    return nil
                }); err != nil {
                    return err
                }                
            }
            if err := txn.Set(key, []byte(val)); err != nil {
                return err
            }
            return nil
        }); err != nil {
            return err
        }
        (*stats).Likes += 1

    } else {
        key := []byte(fmt.Sprintf("unlike:%s!%s", entry.Rkey, entry.DID))
        if err := db.Update(func(txn *badger.Txn) error {
            if err := txn.Set(key, []byte("")); err != nil {
                return err
            }
            return nil
        }); err != nil {
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

    db, err := badger.Open(badger.DefaultOptions("./likes.badger"))
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
        sync := (stats.Entries % SYNC_STEP) == (SYNC_STEP - 1)

        if err := persist(db, entry, &stats); err != nil {
            log.Fatal(err)
        }
        stats.Entries += 1

        if checkin {
            if err := showUpdate(time.Since(t0), "./likes.badger", &stats); err != nil {
                log.Fatal(err)
            }
        }

        // if stats.Entries > 12000 {
        //     break
        // }
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
