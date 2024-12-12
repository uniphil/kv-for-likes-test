use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use fs_extra::dir::get_size;
use rocksdb::{DB, Options, WriteOptions, MergeOperands};
use tinyjson::JsonValue;

const DB_PATH: &str = "./rocks.db";
const LIKES_PATH: &str = "../likes5-simple.jsonl";

const CHECKIN_STEP: u64 = 10_000;
const SYNC_STEP: u64 = 100;

#[derive(Debug, Default)]
struct Stats {
    entries: u64,
    likes: u64,
    unlikes: u64,
    subjects: u64,
}

#[derive(Debug)]
enum Action {
    Create(CreateEntry),
    Delete(DeleteEntry),
}

#[derive(Debug)]
struct CreateEntry {
    did: String,
    rkey: String,
    uri: String,
}

#[derive(Debug)]
struct DeleteEntry {
    did: String,
    rkey: String,
}

impl FromStr for Action {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let parsed: JsonValue = s.parse()?;
        let entry = <Vec<_>>::try_from(parsed)?;
        if entry.len() != 4 {
            panic!("expected entries of length 4");
        }
        let action = String::try_from(entry[0].clone())?;
        let did = String::try_from(entry[1].clone())?;
        let rkey = String::try_from(entry[2].clone())?;
        match action.as_str() {
            "c" => {
                let uri = String::try_from(entry[3].clone())?;
                Ok(Action::Create(CreateEntry { did, rkey, uri }))
            }
            "d" => {
                Ok(Action::Delete(DeleteEntry { did, rkey }))
            }
            _ => Err(anyhow!("need 'c' or 'd' for entry action type"))
        }
    }
}

fn show_update(d: Duration, path: &str, stats: &Stats) {
    let Ok(size) = get_size(path) else {
        return
    };
    println!("{}\t{}\t{:.3}", stats.entries, size, d.as_secs_f32());
}

fn join_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut res = vec![];
    let mut first = true;
    if let Some(ex) = existing_val {
        for b in ex {
            res.push(*b)
        }
        first = false;
    }
    for op in operands {
        if first {
            first = false;
        } else {
            res.push(b';')
        }
        for b in op {
            res.push(*b)
        }
    }
    Some(res)
}

fn main() -> Result<()> {
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let db = DB::open(&{
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("join links", join_merge);
        opts
    }, DB_PATH)?;

    let sync_opts = {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        opts
    };

    let nosync_opts = {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(true);
        opts
    };

    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    {
        let v = db.get(b"at://did:plc:iyr4nadkkq2toocambsr3inz/app.bsky.feed.post/3lccjpbhjck2l")?;
        println!("{}", String::from_utf8(v.unwrap())?);
    }

    if false { for line in reader.lines() {
        let action: Action = line?.parse()?;
        let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
        let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

        let opts = if sync { &sync_opts } else { &nosync_opts };
        match action {
            Action::Create(entry) => {
                let key = &entry.uri.as_bytes();
                let val = format!("{}!{}", entry.did, entry.rkey);
                db.merge_opt(key, &val.as_bytes(), opts)?;
                stats.likes += 1;
            },
            Action::Delete(entry) => {
                let key = format!("{}!{}", entry.did, entry.rkey);
                db.put_opt(&key.as_bytes(), b"", opts)?;
                stats.unlikes += 1;
            },
        }
        stats.entries += 1;

        if checkin {
            show_update(t0.elapsed(), DB_PATH, &stats);
        }
    } }

    db.flush()?;

    let d = t0.elapsed();
    println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
        d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);

    Ok(())
}
