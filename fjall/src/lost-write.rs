use std::fs;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::Instant;
use anyhow::{anyhow, Result};
use fjall::{Config, PersistMode, PartitionCreateOptions};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DB_PATH: &str = "./likes.fjall";
const LIKES_PATH: &str = "../likes5M-anon.txt";

const SYNC_STEP: u64 = 100;

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

fn main() -> Result<()> {
    // ensure a clean start
    fs::remove_dir_all(DB_PATH).or_else(|e| if e.kind() == io::ErrorKind::NotFound { Ok(()) } else { Err(e) })?;

    let reader = io::BufReader::new(fs::File::open(LIKES_PATH)?);

    let mut n_likes = 0;
    let mut n_unlikes = 0;
    let t0 = Instant::now();

    {
        let keyspace = Config::new(DB_PATH).open()?;

        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;

        for line in reader.lines() {
            let action: Action = line?.parse()?;
            let sync = ((n_likes + n_unlikes) % SYNC_STEP) == (SYNC_STEP - 1);

            if sync {
                keyspace.persist(PersistMode::SyncData)?;
            }

            match action {
                Action::Create(key) => {
                    likes.insert(&key, "")?;
                    n_likes += 1;
                }
                Action::Delete(key) => {
                    unlikes.insert(&key, "")?;
                    n_unlikes += 1;
                }
            }
        }

        keyspace.persist(PersistMode::SyncData)?;
        let d = t0.elapsed();
        println!("done in {:.1}s. likes: {}, unlikes: {}", d.as_secs_f32(), n_likes, n_unlikes);

        {
            let n = likes.iter().count();
            if n as u64 != n_likes {
                println!("FAIL: before close: likes {} != {}", n, n_likes);
            }
        }
        {
            let n = unlikes.iter().count();
            if n as u64 != n_unlikes {
                println!("FAIL: before close: unlikes {} != {}", n, n_unlikes);
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
            if n as u64 != n_likes {
                println!("FAIL: after close: likes {} != {}", n, n_likes);
            }
        }
        {
            let n = unlikes.iter().count();
            if n as u64 != n_unlikes {
                println!("FAIL: after close: unlikes {} != {}", n, n_unlikes);
            }
        }

        println!("bye");
    }

    println!("byeeeeee");

    Ok(())
}
