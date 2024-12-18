use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use fjall::{Config, PersistMode, PartitionCreateOptions};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DB_PATH: &str = "./likes.fjall";
const LIKES_PATH: &str = "../likes5M-anon.txt";

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
    Create(String),
    Delete(String),
}

impl FromStr for Action {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let Some((action, rest)) = s.split_once(';') else {
            return Err(anyhow!("line must have at least two fields"))
        };
        match action {
            "c" => {
                let Some((uri, id)) = rest.split_once(';') else {
                    return Err(anyhow!("both uri and id for create"))
                };
                Ok(Action::Create(format!("{}!{}", uri, id)))
            }
            "d" => Ok(Action::Delete(rest.into())),
            _ => Err(anyhow!("need 'c' or 'd' for entry action type")),
        }
    }
}

fn show_update(d: Duration, size: u64, stats: &Stats) {
    println!("{}\t{}\t{:.3}", stats.entries, size, d.as_secs_f32());
}


fn main() -> Result<()> {
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let mut n_likes = 0;
    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    {
        let keyspace = Config::new(DB_PATH).open()?;

        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;

        for line in reader.lines() {
            let action: Action = line?.parse()?;
            let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
            let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

            if sync {
                keyspace.persist(PersistMode::SyncData)?;
            }

            match action {
                Action::Create(key) => {
                    likes.insert(&key, "")?;
                    stats.likes += 1;
                }
                Action::Delete(key) => {
                    unlikes.insert(&key, "")?;
                    stats.unlikes += 1;
                }
            }
            stats.entries += 1;

            if checkin {
                show_update(t0.elapsed(), keyspace.disk_space(), &stats);
            }

            if stats.entries >= 5_000_000 {
                break
            }
        }

        keyspace.persist(PersistMode::SyncData)?;
        let d = t0.elapsed();
        println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
            d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);

        {
            let n = likes.iter().count();
            if n as u64 != stats.likes {
                println!("FAIL: before close: likes {} != {}", n, stats.likes);
            }
        }
        {
            let n = unlikes.iter().count();
            if n as u64 != stats.unlikes {
                println!("FAIL: before close: unlikes {} != {}", n, stats.unlikes);
            }
        }

        println!("closing keyspace...");
    }

    {
        println!("reopening keyspace...");
        let keyspace = Config::new(DB_PATH).open()?;
        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;

        {
            let n = likes.iter().count();
            if n as u64 != stats.likes {
                println!("FAIL: after close: likes {} != {}", n, stats.likes);
            }
        }
        {
            let n = unlikes.iter().count();
            if n as u64 != stats.unlikes {
                println!("FAIL: after close: unlikes {} != {}", n, stats.unlikes);
            }
        }

        println!("bye");
    }

    println!("byeeeeee");

    // TODO: not sure how to count subjects
    Ok(())
}
