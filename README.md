# RTCDB - Robbie's Toy Columnar DataBase

This is a toy DB that I am building to solidify some concepts around how columnar dbs work.

As a starting point, I would recommend reading Designing Data-Intensive Applications (chapter 3 in particular) and [PostHog's Clickhouse manual](https://posthog.com/handbook/engineering/clickhouse)

## Features
It's designed for fast aggregate queries of wide rows, on large (fits on disk on one machine, not in memory) datasets.

 * Store columns separately, so wide rows can be aggregated without needing to read unnecessary columns
 * Column compression, to reduce storage requirements on disk and memory bottlenecks
 * Sparse indexes, allowing large datasets to be read from disk

### Non-features (because it's a toy DB)
* SQL, complex queries, joins, etc
* Replication, backups, etc
* Transactions, locks, etc
* Modifying the schema, migrations, etc
* Most data types (only uint64 and string are supported)

## Architecture
The data for each table is stored in columnar format, i.e. each column is stored in it's own file. If you image for a Table T with columns A B C D, a row based database might store it like
```
t.data:
ABCDABCDABCDABCD
```
Whereas a columnar database will store it like
```
t.A.data:
AAAA

t.B.data:
BBBB

t.C.data:
CCCC

t.D.data:
DDDD
```

This means that it is very efficient to aggregate a small number of columns from a large number of very wide rows (and very inefficient to query all the columns from one row, which is fine you, you would just use a row based DB like Postgres if that's what you wanted).

The rows are sorted according to the column order (i.e. A then B then C then D in the example above). This means that queries that filter based on the sort column (e.g. where A < 100 in the example above)are very fast, as the query engine can ignore the parts of the file that correspond to data that doesn't pass that filter, and seek to the parts that do.

### Blocks and Indexes
Column data is grouped into blocks of 8196 rows, which are also compressed on disk. This can be a variable length, depending on how well the block compresses, but also due to the variable length of some data types (e.g. strings). Compression should typically work very well because the data in one column is typically very similar to each other.

We'd like to be able to binary search the data efficiently, which means we need to keep a separate index. We keep a column index file alongside the column data file, where each entry in the index file corresponds to a block in the data file. These index records are a fixed size, and include the minimum and maximum value in a block, for efficient binary searching and filtering.

### Querying
There are a few stages to querying. We don't support textual SQL queries or joins, which makes this a lot easier than in a non-toy DB.

1. Find the index entries of relevant blocks. To limit the search space, we can use information from the query, e.g. if A is the first column and the query has a clause like A > 100, we can binary search until we find the first matching index entry, then run serially through the index file until an index entry does not match.

2. Further filtering of blocks, based on other parts of the WHERE clause. For example, if A is the first column and B is the second, and the where clause has B < 50, we can use this to decide whether a block should be processed further.

3. Decompressing of the blocks, and filtering using the WHERE clause at a row-level.

4. Collecting the matching rows, and accumulating, grouping, etc, to produce the final result in memory.

Stages 1-3 all stream the data to the following stage, but stage 4 waits until it has all the rows available in memory before calculating the result of the query. The provides an upper limit on the size of the data that can be queried, though this restriction could be worked around in the future by storing intermediate results on disk past a certain size.