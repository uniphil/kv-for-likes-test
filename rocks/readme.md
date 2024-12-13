
building on old sad macos:

```bash
nix-shell -p rocksdb
ROCKSDB_LIB_DIR=/nix/store/z2chn0hsik0clridr8mlprx1cngh1g3c-rocksdb-9.7.3/lib/ cargo build
```

(todo: get the lib dir better)

yay rocks is fast. can we increase space efficiency?

![rocks space efficiency with a few attempts to make the data more info-dense](../doc/rocks-space-denser.png)

![rocks speed with info-dense attempts](../doc/rocks-space-denser-speed.png)
