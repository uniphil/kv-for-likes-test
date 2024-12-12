use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use rocksdb::{DB, Options, MergeOperands, BlockBasedOptions, Cache};

const DB_PATH: &str = "./rocks.db";
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
    let db = DB::open(&{
        let mut opts = Options::default();
        opts.create_if_missing(true);
        // opts.optimize_for_point_lookup(64 * 2_u64.pow(20));
        let cache = Cache::new_lru_cache(64 * 2_usize.pow(20));
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_block_cache(&cache);
        opts.set_block_based_table_factory(&bb_opts);
        opts.set_merge_operator_associative("join links", join_merge);
        opts
    }, DB_PATH)?;

    println!("loop\tduration");
    for n in 0..=2 {
        let reader = io::BufReader::new(File::open(SUBJECTS_PATH)?);

        let mut total = Duration::from_secs(0);
        let mut times: HashMap<usize, Vec<f64>> = HashMap::new();

        for line in reader.lines() {
            let subject: Subject = line?.parse()?;
            let likes = subject.likers.split(';').count();

            let t0 = Instant::now();
            let res = db.get(subject.uri.into_bytes())?.unwrap();
            let d = t0.elapsed();

            total += d;
            (*times.entry(likes).or_insert(vec![])).push(d.as_nanos() as f64);

            assert_eq!(String::from_utf8(res)?, subject.likers);
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
