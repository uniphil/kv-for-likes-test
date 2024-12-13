use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use fjall::{Config, PersistMode, PartitionCreateOptions, Slice};
use tinyjson::JsonValue;

const DB_PATH: &str = "./likes.fjall";
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

fn show_update(d: Duration, size: u64, stats: &Stats) {
    println!("{}\t{}\t{:.3}", stats.entries, size, d.as_secs_f32());
}


fn main() -> Result<()> {
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let keyspace = Config::new(DB_PATH).open_transactional()?;

    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    let likes = keyspace.open_partition("likes", PartitionCreateOptions::default()
        .manual_journal_persist(true)
        .max_memtable_size(32 * 2_u32.pow(20)))?; // 2x default
    let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default()
        .manual_journal_persist(true))?;

    let mut tx = keyspace.write_tx();

    for line in reader.lines() {
        let action: Action = line?.parse()?;
        let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
        let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

        if sync {
            tx.commit()?;
            keyspace.persist(PersistMode::SyncData)?;
            tx = keyspace.write_tx();
        }

        match action {
            Action::Create(entry) => {
                let val = format!("{}!{}", entry.did, entry.rkey);
                let prev = tx.fetch_update(&likes, &entry.uri, |ex| match ex {
                    Some(existing) => Some(Slice::from(format!("{};{}", std::str::from_utf8(existing.as_ref()).ok()?, val))),
                    None => Some(val.clone().into()),
                })?;
                stats.likes += 1;
                if prev.is_none() {
                    stats.subjects += 1;
                }
            }
            Action::Delete(entry) => {
                let key = format!("{}!{}", entry.did, entry.rkey);
                tx.insert(&unlikes, &key, "");
                stats.unlikes += 1;
            }
        }
        stats.entries += 1;

        if checkin {
            show_update(t0.elapsed(), keyspace.disk_space(), &stats);
        }
    }

    tx.commit()?;
    keyspace.persist(PersistMode::SyncData)?;

    let d = t0.elapsed();
    println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
        d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);
    Ok(())
}
