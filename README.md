# PostgreSQL 'Reindex Concurrently'

## Abstract

This script is designed for recreating indexes *concurrently* to tackle *index bloat*. 
Currently it can be processed *by table(s)* and will **replace existing indexes** in a **safe** and **non-locking** fashion.
This is achieved by leveraging `CREATE INDEX CONCURRENTLY` + test validity 
(as it can fail and leave behind an invalid index) + retry/cleanup in case of failure - but ultimately replaces
(`DROP` old, `RENAME` new index) indexes with new ones.

Works both for regular btree indexes and primary keys.  

Takes a timeout so that it won't overrun your slow traffic period.
Note that this is the time to START a CREATE INDEX CONCURRENTLY, so a large index
may still overrun the timeout period, unless you use the --enforce-time switch.

Collects stats and outputs information on index disk storage size before & after the reindexing. 
When finished (or interrupted due to scheduled timeout period) outputs aggregated results for all processed indexes. 

Reference: https://www.postgresql.org/docs/current/static/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY


## Known supported PostgreSQL versions
- PostgreSQL 10 (any)


## Use

    $ python scripts/reindex_concurrently.py -h
    usage: reindex_concurrently.py [-h] [-t TABLES] [-i INDEXES] [-H DBHOST]
                                   [-p DBPORT] -d DB [-U DBUSER] [-w DBPASS]
                                   [-I IGNORED] [-m RUN_MIN] [--enforce-time]
                                   [-r RETRIES] [--dry-run] [--pause PAUSE_TIME]
                                   [--print-timestamps] [-l LOGFILE] [-v]
                                   [--debug]
    
    If an arg is specified in more than one place, then commandline values
    override environment variables which override defaults.
    
    optional arguments:
      -h, --help            show this help message and exit
      -t TABLES, --tables TABLES
                            Comma-separated list of tables to reindex
                            concurrently. [env var: TABLES]
      -i INDEXES, --indexes INDEXES
                            Comma-separated list of indexes to reindex
                            concurrently. [env var: INDEXES]
      -H DBHOST, --host DBHOST
                            database hostname [env var: HOST]
      -p DBPORT, --port DBPORT
                            database port [env var: PORT]
      -d DB, --database DB  Database to connect to. [env var: DATABASE]
      -U DBUSER, --user DBUSER
                            database user [env var: USER]
      -w DBPASS, --password DBPASS
                            database password [env var: PASSWORD]
      -I IGNORED, --ignore-indexes IGNORED
                            Comma-separated list of indexes to ignore and skip
                            while processing tables. [env var: IGNORE_INDEXES]
      -m RUN_MIN, --minutes RUN_MIN
                            Number of minutes to run before halting. Defaults to 2
                            hours [env var: MINUTES]
      --enforce-time        enforce time limit by terminating running queries [env
                            var: ENFORCE_TIME]
      -r RETRIES, --retries RETRIES
                            Retry attempts if CREATE INDEX CONCURRENTLY fails
                            (invalid). [env var: RETRIES]
      --dry-run             dry run - print SQL statements only [env var: DRY_RUN]
      --pause PAUSE_TIME    seconds to pause between reindexing. Default is 5.
                            [env var: PAUSE]
      --print-timestamps    [env var: PRINT_TIMESTAMPS]
      -l LOGFILE, --log LOGFILE
      -v, --verbose         [env var: VERBOSE]
      --debug               [env var: DEBUG]

 
## Docker

### Build
    
    docker build -t hartmutcouk/pg-reindex-concurrently .
    
### Run

    docker run -it --rm -e "TABLES=table1,table2" -e "HOST=localhost" -e "USER=dbuser" -e "MINUTES=180" -e "PASSWORD=secret" -e "DATABASE=dbname" -e "PRINT_TIMESTAMPS=1" -e "DRY_RUN=True" -e "PAUSE=0" --name pg-reindex-concurrently-job-01 pg-reindex-concurrently
    
### Dockerhub

Public docker image available on [Docker Hub](https://hub.docker.com/r/hartmutcouk/pg-reindex-concurrently/): `hartmutcouk/pg-reindex-concurrently`
    

## Alternative -> PG Extension 'pg_repack'

There's a PostgreSQL extension 'pg_repack' for
> Reorganize tables in PostgreSQL databases with minimal locks
Which comes with similar functionality (see `-x, --only-indexes`) ++ actual relocating of whole tables in order of _CLUSTER_.

https://github.com/reorg/pg_repack

### pg_repack on RDS, PG10

Since ~January 2018 RDS does support the 'pg_repack' extension with v1.4.2 - which (as of the time writing this readme 2018-07-05) doesn't work with PG10 - therefore unusuable for PG10+

#### References:
- https://forums.aws.amazon.com/thread.jspa?threadID=284193
- https://github.com/reorg/pg_repack/issues/169
- https://github.com/reorg/pg_repack/issues/182


## TODOs
- [x] provide public docker image on https://hub.docker.com 
- [ ] allow to start for a whole database and|or schema


## Credits

The script has been inspired and originally forked from https://github.com/pgexperts/flexible-freeze

