use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use redb::{Database, TableDefinition, WriteTransaction, ReadableTable, DatabaseStats};
use tinyjson::JsonValue;

const DB_PATH: &str = "./likes.redb";
const LIKES_PATH: &str = "../likes5-simple.jsonl";

const CHECKIN_STEP: u64 = 10_000;
const SYNC_STEP: u64 = 100;

const LIKES: TableDefinition<&str, &str> = TableDefinition::new("likes");
const UNLIKES: TableDefinition<&str, ()> = TableDefinition::new("unlikes");

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


fn persist_like(tx: &WriteTransaction, action: CreateEntry, stats: &mut Stats) -> Result<()> {
    let mut val = format!("{}!{}", action.did, action.rkey);
    let mut table = tx.open_table(LIKES)?;
    if let Some(existing) = table.get(&*action.uri)? {
        val = format!("{};{}", existing.value(), val);
    } else {
        stats.subjects += 1;
    }
    table.insert(&*action.uri, &*val)?;
    stats.likes += 1;
    Ok(())
}

fn persist_unlike(tx: &WriteTransaction, action: DeleteEntry, stats: &mut Stats) -> Result<()> {
    let key = format!("{}!{}", action.did, action.rkey);
    tx.open_table(UNLIKES)?.insert(&*key, ())?;
    stats.unlikes += 1;
    Ok(())
}

fn show_update(d: Duration, db_stats: DatabaseStats, stats: &Stats) {
    let total_size = db_stats.stored_bytes() + db_stats.metadata_bytes() + db_stats.fragmented_bytes();
    println!("{}\t{}\t{:.3}", stats.entries, total_size, d.as_secs_f32());
}

fn main() -> Result<()> {
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let db = Database::create(DB_PATH)?;

    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    let mut tx = db.begin_write()?;

    for line in reader.lines() {
        let action: Action = line?.parse()?;
        let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
        let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

        if sync {
            tx.commit()?;
            tx = db.begin_write()?;
        }

        match action {
            Action::Create(entry) => persist_like(&tx, entry, &mut stats)?,
            Action::Delete(entry) => persist_unlike(&tx, entry, &mut stats)?,
        }
        stats.entries += 1;

        if checkin {
            show_update(t0.elapsed(), tx.stats()?, &stats);
        }
    }

    tx.commit()?;

    let d = t0.elapsed();
    println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
        d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);

    Ok(())
}
