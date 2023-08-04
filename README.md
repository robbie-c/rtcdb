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
