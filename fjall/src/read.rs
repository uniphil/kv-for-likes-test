use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use fjall::{Config, PartitionCreateOptions, BlockCache};

const DB_PATH: &str = "./likes.fjall";
const SUBJECTS_PATH: &str = "../sampled-subjects-100k.txt";

#[derive(Debug)]
struct Subject {
    uri: String,
    likers: String,
}

impl FromStr for Subject {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        if let Some((uri, likers)) = s.split_once('|') {
            Ok(Subject { uri: uri.into(), likers: likers.into() })
        } else {
            Err(anyhow!("failed to split input"))
        }
    }
}

fn main() -> Result<()> {
    let keyspace = Config::new(DB_PATH)
        .block_cache(BlockCache::with_capacity_bytes(64 * 2_u64.pow(20)).into())
        .open()?;

    let likes = keyspace.open_partition("likes", PartitionCreateOptions::default().block_size(32 * 2_u32.pow(10)))?;

    println!("loop\tduration");
    for n in 0..=2 {
        let reader = io::BufReader::new(File::open(SUBJECTS_PATH)?);

        let mut total = Duration::from_secs(0);
        let mut times: HashMap<usize, Vec<f64>> = HashMap::new();

        for line in reader.lines() {
            let subject: Subject = line?.parse()?;
            let n_likes = subject.likers.split(';').count();

            let t0 = Instant::now();
            let db_n_likes = likes.prefix(&subject.uri).count();
            let d = t0.elapsed();

            total += d;
            (*times.entry(n_likes).or_insert(vec![])).push(d.as_nanos() as f64);

            assert_eq!(db_n_likes, n_likes);
        }
        println!("{n}\t{:.3}", total.as_secs_f32());

        let mut res: Vec<_> = times
            .iter()
            .map(|(likes, group)|
                (likes, group.into_iter().sum::<f64>() / (group.len() as f64) / 1000.0))
            .collect();
        res.sort_by(|a, b| a.partial_cmp(b).unwrap());
        for (likes, micros) in res {
            println!("{likes}\t{micros:.3}");
        }
    }

    Ok(())
}
