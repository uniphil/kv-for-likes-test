use std::fs::File;
use std::io::{self, BufRead};
use std::str::FromStr;
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use fs_extra::dir::get_size;
use rocksdb::{DB, Options, WriteOptions, MergeOperands, ColumnFamilyDescriptor, WriteBatch};
use tinyjson::JsonValue;

const DB_PATH: &str = "./normed.rocks";
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

#[derive(Debug, PartialEq)]
enum AtUri {
    Did(String),
    DidCollection(String, String),
    DidCollectionKey(String, String, String),
}

impl FromStr for AtUri {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let Some(uri) = s.strip_prefix("at://") else {
            return Err(anyhow!("at-uri must start with at://"))
        };
        if !uri.starts_with("did:") {
            return Err(anyhow!("at-uri id must begin with 'did:'"))
        }
        let Some((did, collection)) = uri.split_once('/') else {
            return Ok(AtUri::Did(uri.to_string()))
        };
        let Some((collection, rkey)) = collection.split_once('/') else {
            return Ok(AtUri::DidCollection(did.to_string(), collection.to_string()))
        };
        Ok(AtUri::DidCollectionKey(did.to_string(), collection.to_string(), rkey.to_string()))
    }
}

impl ToString for AtUri {
    fn to_string(&self) -> String {
        format!("at://{}", match self {
            AtUri::Did(did) => did.clone(),
            AtUri::DidCollection(did, col) => format!("{did}/{col}"),
            AtUri::DidCollectionKey(did, col, rkey) => format!("{did}/{col}/{rkey}"),
        })
    }
}


fn show_update(d: Duration, path: &str, stats: &Stats) {
    let Ok(size) = get_size(path) else {
        return
    };
    println!("{}\t{}\t{:.3}", stats.entries, size, d.as_secs_f32());
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
    let reader = io::BufReader::new(File::open(LIKES_PATH)?);

    let ids_cf_d = ColumnFamilyDescriptor::new("ids", Options::default());
    let links_cf_d = ColumnFamilyDescriptor::new("links", {
        let mut opts = Options::default();
        opts.set_merge_operator_associative("join links", join_merge);
        opts
    });
    let db = DB::open_cf_descriptors(
        &{
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts
        },
        DB_PATH,
        vec![ids_cf_d, links_cf_d],
    )?;

    let ids_cf = db.cf_handle("ids").unwrap();
    let links_cf = db.cf_handle("links").unwrap();

    let sync_opts = {
        let mut opts = WriteOptions::default();
        opts.set_sync(true);
        opts
    };

    let nosync_opts = {
        let mut opts = WriteOptions::default();
        opts.set_sync(false);
        opts.disable_wal(true);
        opts
    };

    let mut current_id_seq = db.get_cf(&ids_cf, b"id.seq")?
        .map(|existing| u64::from_le_bytes(existing.try_into().unwrap()))
        .unwrap_or_else(|| {
            println!("no initial db seq found: starting at 0");
            0
        });

    let mut next_id = |batch: &mut WriteBatch| {
        let yours = current_id_seq.to_le_bytes();
        current_id_seq += 1;
        batch.put_cf(&ids_cf, b"id.seq", current_id_seq.to_le_bytes());
        yours
    };

    let mut stats: Stats = Default::default();
    let t0 = Instant::now();

    for line in reader.lines() {
        let action: Action = line?.parse()?;
        let checkin = (stats.entries % CHECKIN_STEP) == (CHECKIN_STEP - 1);
        let sync = (stats.entries % SYNC_STEP) == (SYNC_STEP - 1);

        let opts = if sync { &sync_opts } else { &nosync_opts };
        match action {
            Action::Create(entry) => {
                let mut batch = WriteBatch::default();

                let actual_linking_did = entry.did.as_bytes();
                let linking_did_id = db.get_cf(&ids_cf, &actual_linking_did)?
                    .unwrap_or_else(|| {
                        let id = next_id(&mut batch);
                        batch.put_cf(&ids_cf, &actual_linking_did, id);
                        id.to_vec()
                    });

                let at_uri: AtUri = entry.uri.parse()?;
                let AtUri::DidCollectionKey(actual_target_did, actual_collection, rkey) = at_uri else {
                    panic!("expected did/collection/rkey uri");
                };

                let target_did_id = db.get_cf(&ids_cf, &actual_target_did)?
                    .unwrap_or_else(|| {
                        let id = next_id(&mut batch);
                        batch.put_cf(&ids_cf, &actual_target_did, id);
                        id.to_vec()
                    });

                let collection_id = db.get_cf(&ids_cf, &actual_collection)?
                    .unwrap_or_else(|| {
                        let id = next_id(&mut batch);
                        batch.put_cf(&ids_cf, &actual_collection, id);
                        id.to_vec()
                    });

                let actual_smol_uri = [target_did_id, collection_id, rkey.into_bytes()].concat();
                let uri_id = db.get_cf(&ids_cf, &actual_smol_uri)?
                    .unwrap_or_else(|| {
                        let id = next_id(&mut batch);
                        batch.put_cf(&ids_cf, &actual_smol_uri, id);
                        id.to_vec()
                    });

                let mut link_key = linking_did_id.clone();
                link_key.push(b':');
                link_key.extend_from_slice(entry.rkey.as_bytes());

                batch.put_cf(&links_cf, &link_key, &uri_id);
                batch.merge_cf(&links_cf, &uri_id, &linking_did_id);

                db.write_opt(batch, opts)?;
                stats.likes += 1;
            },
            Action::Delete(entry) => {
                let mut batch = WriteBatch::default();

                let actual_did = entry.did.as_bytes();
                let Some(did_id) = db.get_cf(&ids_cf, &actual_did)? else {
                    // we don't have this link to delete
                    continue
                };

                let mut link_key = did_id.to_vec();
                link_key.push(b':');
                link_key.extend_from_slice(entry.rkey.as_bytes());

                let Some(uri_id) = db.get_cf(&ids_cf, &link_key)? else {
                    // delete link to uri we never had -- if we're backfilled this is a weirder thing to happen
                    continue
                };

                let Some(_likes) = db.get_cf(&ids_cf, &uri_id)? else {
                    eprintln!("failed to resolve link id to a uri -- likely a bug");
                    continue
                };

                // TODO: actually remove this did from the likes list(s) for that uri
                batch.delete_cf(&links_cf, &link_key);

                stats.unlikes += 1;
            },
        }
        stats.entries += 1;

        if checkin {
            show_update(t0.elapsed(), DB_PATH, &stats);
        }

        // if stats.entries > 24000 {
        //     break
        // }
    }

    db.flush()?;

    let d = t0.elapsed();
    println!("done in {:.1}s. entries: {}, likes: {}, unlikes: {}, subjects: {}",
        d.as_secs_f32(), stats.entries, stats.likes, stats.unlikes, stats.subjects);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_at_uri_did() {
        let uri = "at://did:plc:hdhoaan3xa3jiuq4fg4mefid";
        let did = "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string();
        let parsed: AtUri = uri.parse().unwrap();
        assert_eq!(parsed, AtUri::Did(did));
        assert_eq!(parsed.to_string(), uri);
    }

    #[test]
    fn test_at_uri_did_col() {
        let uri = "at://did:plc:hdhoaan3xa3jiuq4fg4mefid/app.bsky.actor.profile";
        let did = "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string();
        let col = "app.bsky.actor.profile".to_string();
        let parsed: AtUri = uri.parse().unwrap();
        assert_eq!(parsed, AtUri::DidCollection(did, col));
        assert_eq!(parsed.to_string(), uri);
    }

    #[test]
    fn test_at_uri_did_col_rkey() {
        let uri = "at://did:plc:hdhoaan3xa3jiuq4fg4mefid/app.bsky.feed.like/3ld53lnvvhc2w";
        let did = "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string();
        let col = "app.bsky.feed.like".to_string();
        let rkey = "3ld53lnvvhc2w".to_string();
        let parsed: AtUri = uri.parse().unwrap();
        assert_eq!(parsed, AtUri::DidCollectionKey(did, col, rkey));
        assert_eq!(parsed.to_string(), uri);
    }
}
