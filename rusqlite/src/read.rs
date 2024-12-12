use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use rusqlite::Connection;

const DB_PATH: &str = "./likes.db";
const SUBJECTS_PATH: &str = "../sampled-subjects-100k.txt";

const MB_IN_KB: i64 = 2_i64.pow(10);
const READ_CACHE: i64 = 64 * MB_IN_KB;

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
    let conn = Connection::open(DB_PATH)?;
    conn.pragma_update(None, "cache_size", (-READ_CACHE).to_string())?;

    let mut stmt = conn.prepare("SELECT cast(likes as TEXT) FROM likes WHERE uri = ?1")?;

    println!("loop\tduration");
    for n in 0..=2 {
        let reader = io::BufReader::new(File::open(SUBJECTS_PATH)?);

        let mut total = Duration::from_secs(0);
        let mut times: HashMap<usize, Vec<f64>> = HashMap::new();

        for line in reader.lines() {
            let subject: Subject = line?.parse()?;
            let likes = subject.likers.split(';').count();

            let t0 = Instant::now();
            let db_likers: String = stmt.query_row((subject.uri.into_bytes(),), |row| row.get(0))?;
            let d = t0.elapsed();

            total += d;
            (*times.entry(likes).or_insert(vec![])).push(d.as_nanos() as f64);

            assert_eq!(db_likers, subject.likers);
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
