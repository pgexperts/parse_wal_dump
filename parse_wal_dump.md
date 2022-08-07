# parse_wal_dump

This is a simple Python script that takes the output of `pg_waldump` on standard input,
parses it, and inserts records into a database with a subset of the information that
is in the WAL file. It only parses and inserts records that can be attributed to
a specific relation; other information is rejected.

It takes a single argument, which is a connection string to the databse to which
to insert the data. If the table to hold the parsed data does not exist, it
creates it; the schema is:

```
    CREATE TABLE IF NOT EXISTS wal_usage(
        lsn text primary key,
        rmgr text,
        len_rec int,
        len_tot int,
        tablespace int,
        database int,
        relation int
```

The main purpose of this script is to analyze WAL information to get an idea of 
how much changes to particular relations are contributing to the WAL volume.