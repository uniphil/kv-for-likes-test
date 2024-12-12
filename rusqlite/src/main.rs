use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use rusqlite::{Connection, TransactionBehavior};
use tinyjson::JsonValue;

const DB_PATH: &str = "./likes.db";
const LIKES_PATH: &str = "../likes5-simple.jsonl";

const CHECKIN_STEP: u64 = 10_000;
const SYNC_STEP: u64 = 100;

const MB_IN_KB: i64 = 2_i64.pow(10);
const WRITE_CACHE: i64 = 100 * MB_IN_KB;

const ADD_STATEMENT: &str =
    "INSERT INTO likes (uri, likes) VALUES (?1, ?2)
        ON CONFLICT DO UPDATE
        SET likes = likes || ';' || ?2";

const DEL_STATEMENT: &str =
    "INSERT INTO unlikes (did_rkey) VALUES (?1)
        ON CONFLICT DO NOTHING";

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

fn show_update(d: Duration, db_path: &Path, stats: &Stats) {
    let Ok(size) = db_path.metadata().map(|m| m.len()) else {
        return
    };
    println!("{}\t{}\t{:.3}", stats.entries, size, d.as_secs_f32());
}

fn main() -> Result<()> {
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let mut conn = Connection::open(DB_PATH)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;

    // 1.5G cache size: didn't help
    // removing without rowid: helped!? total runtime 6h -> 5.2h, maintained over 500/sec
    // blobs: possible tiny improvement, but very very small
    // wal_autocheckpoint: massive speedup up to ~5M entries, falling to no improvement by ~14M
    // threads: nothing measurable up to ~6.5M entries, ended test early

    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "cache_size", (-WRITE_CACHE).to_string())?;
    conn.pragma_update(None, "busy_timeout", "100")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS likes (
            uri   blob PRIMARY KEY,
            likes blob NOT NULL
        )",
        (),
    ).expect("create likes table");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS unlikes (
            did_rkey blob PRIMARY KEY
        )",
        (),
    ).expect("create unlikes table");

    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    let mut tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut add_statement = tx.prepare_cached(ADD_STATEMENT)?;
    let mut del_statement = tx.prepare_cached(DEL_STATEMENT)?;

    for line in reader.lines() {
        let action: Action = line?.parse()?;
        let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
        let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

        if sync {
            drop(add_statement);
            drop(del_statement);
            tx.commit()?;
            tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            add_statement = tx.prepare_cached(ADD_STATEMENT)?;
            del_statement = tx.prepare_cached(DEL_STATEMENT)?;
        }

        match action {
            Action::Create(entry) => {
                let val = format!("{}!{}", entry.did, entry.rkey);
                add_statement.execute((entry.uri.into_bytes(), val.into_bytes()))?;
                stats.likes += 1;
                // TODO: subjects. could get there with RETURNING but for now will just query at the end.
                // https://sqlite.org/forum/info/e88687aeaecf9528
            }
            Action::Delete(entry) => {
                let key = format!("{}!{}", entry.did, entry.rkey);
                del_statement.execute((key.into_bytes(),))?;
                stats.unlikes += 1;
            }
        }
        stats.entries += 1;

        if checkin {
            show_update(t0.elapsed(), DB_PATH.as_ref(), &stats);
        }
    }

    drop(add_statement);
    drop(del_statement);
    tx.commit()?;

    let d = t0.elapsed();

    stats.subjects = conn.query_row("SELECT count(*) FROM likes", [], |r| r.get(0))?;

    println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
        d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);

    Ok(())
}
