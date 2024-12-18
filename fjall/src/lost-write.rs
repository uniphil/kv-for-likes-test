use std::fs;
use std::io::{self, BufRead};
use std::time::Instant;
use fjall::{Config, PartitionHandle, PersistMode, PartitionCreateOptions};

const DB_PATH: &str = "./likes.fjall";
const LIKES_PATH: &str = "../likes5M-anon.txt";

fn check_count(part: PartitionHandle, expected: usize) {
    print!("checking {}: ", part.name);
    let counted = part.len().unwrap();
    if counted == expected {
        println!("OK:   {counted} == {expected}");
    } else {
        println!("FAIL: {counted} != {expected}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    fs::remove_dir_all(DB_PATH).or_else(|e| // ensure a clean start
        if e.kind() == io::ErrorKind::NotFound { Ok(()) } else { Err(e) })?;

    let mut n_likes = 0;
    let mut n_unlikes = 0;

    {
        let reader = io::BufReader::new(fs::File::open(LIKES_PATH)?);
        let keyspace = Config::new(DB_PATH).open()?;
        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;

        let t0 = Instant::now();
        for line in reader.lines() {
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
    }

    {
        println!("reopening keyspace...");
        let keyspace = Config::new(DB_PATH).open()?;
        let likes = keyspace.open_partition("likes", PartitionCreateOptions::default())?;
        let unlikes = keyspace.open_partition("unlikes", PartitionCreateOptions::default())?;
        check_count(likes, n_likes);
        check_count(unlikes, n_unlikes);
    }

    Ok(())
}
