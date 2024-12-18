use std::fs;
use std::io::{self, BufRead};
use std::time::Instant;
use fjall::{Config, PartitionHandle, PersistMode, PartitionCreateOptions};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DB_PATH: &str = "./likes.fjall";
const LIKES_PATH: &str = "../likes5M-anon.txt";

const SYNC_STEP: usize = 100;


fn check_count(part: PartitionHandle, expected: usize) {
    let counted = part.len().unwrap();
    if counted != expected {
        println!("FAIL: {}: found {counted} != {expected}", part.name);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            if ((n_likes + n_unlikes) % SYNC_STEP) == (SYNC_STEP - 1) {
                keyspace.persist(PersistMode::SyncData)?;
            }
            match line?.split_once(';').expect("line must have two fields") {
                ("c", k) => {
                    likes.insert(&k, "")?;
                    n_likes += 1;
                }
                ("d", k) => {
                    unlikes.insert(&k, "")?;
                    n_unlikes += 1;
                }
                (_, _) => panic!("action must be 'c' or 'd'"),
            }
        }

        keyspace.persist(PersistMode::SyncData)?;
        let d = t0.elapsed();
        println!("done in {:.1}s. likes: {}, unlikes: {}", d.as_secs_f32(), n_likes, n_unlikes);

        check_count(likes, n_likes);
        check_count(unlikes, n_unlikes);

        println!("closing keyspace...");
    }

    {
        println!("reopening keyspace...");
        let keyspace = Config::new(DB_PATH).open()?;
        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;

        check_count(likes, n_likes);
        check_count(unlikes, n_unlikes);

        println!("bye");
    }

    println!("byeeeeee");

    Ok(())
}
