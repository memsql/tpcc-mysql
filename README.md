# TPC-C Driver for MySQL Compliant Databases #

This driver is forked from github.com/Percona-Lab/tpcc-mysql

# Build Binaries #

Running `make -C src/` will build two binaries: `tpcc_load` and `tpcc_start`.

# Generate Dataset #

The `tpcc_load` binary can be invoked to generate arbitrarily sized datasets, and can be parallelized
in multiple ways. On every invocation, the `-w` (warehouse count), `-m` (minimum warehouse # to start
generating data for), and `-n` (warehouse # to stop generating data at) flags must be passed. These can
be used to generate only the slices of data that belong to those warehouses. Second, the `-l` flag can
passed to generate data for only a subset of tables; passing no `-l` flag at all will cause all data
types to be generated, while passing 1 - 4 will create the corresponding subset.

While running, the generator will write files to disk in the format `{table}.{min warehouse}.{chunk #}`.
As files reach the size limit (20MB), the completed chunk's filename is written to stderr and the next
chunk is begun. This allows scripting parallelized gzip and upload of the pipe separated value files.

# Create a database #

This repository contains schemas and indices for all tables. A single DDL file with a schema optimized
for MemSQL is provided as `memsql_create_table.sql`, while `create_table.sql` and `add_fkey_idx.sql` are
for MySQL. If using MemSQL, be sure to tune the replication, durability, and replication settings when
you create your database. Run the DDL files against your database
(`mysql -h 127.0.0.1 ... database < memsql_create_table.sql`).

# Loading data #

The dataset created by the generator is in pipe-separated values format, which can be loaded via `LOAD
DATA` using either MySQL or MemSQL. MemSQL also has functionality for 'pipelines', which will allow you
to easily and rapidly load the data from a cloud data store like S3. Documentation is available online:
https://docs.memsql.com/memsql-pipelines/v6.7/pipelines-overview/

# Run the Benchmark #

The `tpcc_start` binary will run the actual workload against your database. It takes the following flags:

* `-h`: host to connect to
* `-P`: port to connect to
* `-d`: database to connect to
* `-u`: user to connect as
* `-p`: password to use
* `-w`: number of warehouses to run against
* `-r`: warmup time (in seconds) before starting to gather results
* `-c`: connections to run in parallel
* `-i`: interval (in seconds) to report intermittent results
* `-l`: duration to run the benchmark for (in seconds)

Output
===================================

With the defined interval (-i option), the tool will produce the following output:
```
  10, trx: 12920, 95%: 9.483, 99%: 18.738, max_rt: 213.169, 12919|98.778, 1292|101.096, 1293|443.955, 1293|670.842
  20, trx: 12666, 95%: 7.074, 99%: 15.578, max_rt: 53.733, 12668|50.420, 1267|35.846, 1266|58.292, 1267|37.421
  30, trx: 13269, 95%: 6.806, 99%: 13.126, max_rt: 41.425, 13267|27.968, 1327|32.242, 1327|40.529, 1327|29.580
  40, trx: 12721, 95%: 7.265, 99%: 15.223, max_rt: 60.368, 12721|42.837, 1271|34.567, 1272|64.284, 1272|22.947
  50, trx: 12573, 95%: 7.185, 99%: 14.624, max_rt: 48.607, 12573|45.345, 1258|41.104, 1258|54.022, 1257|26.626
```

Where: 
* 10 - the seconds from the start of the benchmark
* trx: 12920 - New Order transactions executed during the gived interval (in this case, for the previous 10 sec). Basically this is the throughput per interval. The more the better
* 95%: 9.483: - The 95% Response time of New Order transactions per given interval. In this case it is 9.483 sec
* 99%: 18.738: - The 99% Response time of New Order transactions per given interval. In this case it is 18.738 sec
* max_rt: 213.169: - The Max Response time of New Order transactions per given interval. In this case it is 213.169 sec
* the rest: `12919|98.778, 1292|101.096, 1293|443.955, 1293|670.842` is throughput and max response time for the other kind of transactions and can be ignored
