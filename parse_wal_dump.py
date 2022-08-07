#!/usr/bin/env python3

import os
import re
import sys
import argparse

import psycopg2

parser = argparse.ArgumentParser(description='Parse WAL segments and insert them into a database.')
parser.add_argument('connstr', help='connection string for the target database')

args = parser.parse_args()

#

waldump_line_pattern = re.compile(r'rmgr: (?P<rmgr>[A-Za-z]+).+len \(rec\/tot\)\: *(?P<len_rec>\d+) *\/ *(?P<len_tot>\d+),' +\
     '.+lsn\: (?P<lsn>[0-9A-F]+\/[0-9A-F]+),' +\
     '.+rel (?P<tablespace>\d+)/(?P<database>\d+)/(?P<relation>\d+) ')

conn = psycopg2.connect(args.connstr)
conn.autocommit = True

#

c = conn.cursor()

c.execute(""" 
    CREATE TABLE IF NOT EXISTS wal_usage(
        lsn text primary key,
        rmgr text,
        len_rec int,
        len_tot int,
        tablespace int,
        database int,
        relation int
    )
    """)
    
#

lines_accepted = 0
lines_rejected = 0

for line in sys.stdin:
    parsed = waldump_line_pattern.match(line)
    if not parsed:
        lines_rejected += 1
        continue

    lines_accepted += 1

    c.execute("""INSERT INTO wal_usage(rmgr, len_rec, len_tot, lsn, tablespace, database, relation)
        VALUES(%(rmgr)s, %(len_rec)s, %(len_tot)s, %(lsn)s, %(tablespace)s, %(database)s, %(relation)s)""",
            { 'rmgr': parsed.group('rmgr'),
              'len_rec': int(parsed.group('len_rec')),
              'len_tot': int(parsed.group('len_tot')),
              'lsn': parsed.group('lsn'),
              'tablespace': int(parsed.group('tablespace')),
              'database': int(parsed.group('database')),
              'relation': int(parsed.group('relation')),
            }
        )

print(f"{lines_accepted} lines successfully parsed, {lines_rejected} lines rejected")