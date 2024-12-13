use rocksdb::{DB, Options, MergeOperands, ColumnFamilyDescriptor, WriteBatch};
use anyhow::{anyhow, Result};


const IDS_CF_NAME: &str = "ids";
const IDS_SEQ_KEY: &[u8] = b"id seq";


pub struct Store {
    db: DB,
    ids: StoreIdSeq,
}

struct StoreIdSeq {
    current_id: u64,
}

#[derive(Debug)]
struct StoreID(u64);

fn join_id_merge(
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

impl StoreIdSeq {
    pub fn new(db: &mut DB) -> Result<Self> {
        let ids_cf = db.cf_handle(IDS_CF_NAME).expect("db must have ids column family");
        let current_id = match db.get_cf(&ids_cf, IDS_SEQ_KEY)? {
            Some(existing) => {
                let Ok(bytes): std::result::Result<[u8; 8], _> = existing.try_into() else {
                    return Err(anyhow!("failed to get 8 bytes for u64 conversion for id sequence"))
                };
                u64::from_le_bytes(bytes)
            }
            None => {
                println!("no initial db seq found: starting at 0");
                0
            }
        };
        Ok(StoreIdSeq { current_id })
    }

    pub fn next(&mut self, db: &mut DB, batch: &mut WriteBatch) -> StoreID {
        let ids_cf = db.cf_handle(IDS_CF_NAME).expect("db must have ids column family");
        let yours = StoreID(self.current_id);
        self.current_id += 1;
        batch.put_cf(&ids_cf, IDS_SEQ_KEY, self.current_id.to_le_bytes());
        yours
    }
}

impl Store {
    pub fn new(path: &str) -> Result<Self> {
        let ids_cf_d = ColumnFamilyDescriptor::new("ids", Options::default());
        let links_cf_d = ColumnFamilyDescriptor::new("links", {
            let mut opts = Options::default();
            opts.set_merge_operator_associative("join links", join_id_merge);
            opts
        });
        let mut db = DB::open_cf_descriptors(
            &{
                let mut opts = Options::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                opts
            },
            path,
            vec![ids_cf_d, links_cf_d],
        )?;
        let ids = StoreIdSeq::new(&mut db)?;

        Ok(Store { db, ids })
    }

    pub fn next(&mut self) -> Result<u64> {
        let mut batch = WriteBatch::default();
        let id = self.ids.next(&mut self.db, &mut batch);
        self.db.write(batch)?;
        Ok(id.0)
    }
}


fn add(a: u32, b: u32) -> u32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 3);
    }

    #[test]
    fn test_store() {
        let mut store = Store::new("test.rocks").unwrap();
        assert_eq!(store.next().unwrap(), 1);
    }
}
