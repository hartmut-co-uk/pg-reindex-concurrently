'''Reindex Concurrently script for PostgreSQL databases
Version 1.1.0
(c) 2018 Hartmut Armbruster <info@hartmut.co.uk>

This script is designed for recreating indexes *concurrently* to tackle *index bloat*.
Currently it can be processed *by table(s)* and will **replace existing indexes** in a **safe** and **non-locking** fashion.
This is achieved by leveraging `CREATE INDEX CONCURRENTLY` + test validity
(as it can fail and leave behind an invalid index) + retry/cleanup in case of failure - but ultimately replaces
(`DROP` old, `RENAME` new index) indexes with new ones.

Works both for regular btree indexes and primary keys.

Takes a timeout so that it won't overrun your slow traffic period.
Note that this is the time to START a CREATE INDEX CONCURRENTLY, so a large index
may still overrun the timeout period, unless you use the --enforce-time switch.

Reference: https://www.postgresql.org/docs/current/static/sql-createindex.html#SQL-CREATEINDEX-CONCURRENTLY
'''

import datetime
import signal
import sys
import time

import configargparse
import psycopg2
import humanfriendly
from psycopg2._psycopg import QueryCanceledError


def timestamp():
    now = time.time()
    return time.strftime("%Y-%m-%d %H:%M:%S %Z")


if sys.version_info[:2] not in ((2, 6), (2, 7),):
    print >> sys.stderr, "python 2.6 or 2.7 required; you have %s" % sys.version
    exit(1)

parser = configargparse.ArgumentParser()
parser.add_argument("-t", "--tables", dest="tables", help="Comma-separated list of tables to reindex concurrently.", env_var="TABLES")
parser.add_argument("-i", "--indexes", dest="indexes", help="Comma-separated list of indexes to reindex concurrently.", env_var="INDEXES")
parser.add_argument("-H", "--host", dest="dbhost", help="database hostname", env_var="HOST")
parser.add_argument("-p", "--port", dest="dbport", help="database port", env_var="PORT")
parser.add_argument("-d", "--database", required='True', dest="db", help="Database to connect to.", env_var="DATABASE")
parser.add_argument("-U", "--user", dest="dbuser", help="database user", env_var="USER")
parser.add_argument("-w", "--password", dest="dbpass", help="database password", env_var="PASSWORD")
parser.add_argument("-I", "--ignore-indexes", dest="ignored", help="Comma-separated list of indexes to ignore and skip while processing tables.", env_var="IGNORE_INDEXES")
parser.add_argument("-m", "--minutes", dest="run_min", type=int, default=120, help="Number of minutes to run before halting. Defaults to 2 hours", env_var="MINUTES")
parser.add_argument("--enforce-time", dest="enforcetime", action="store_true", help="enforce time limit by terminating running queries", env_var="ENFORCE_TIME")
parser.add_argument("-r", "--retries", dest="retries", type=int, default=2, help="Retry attempts if CREATE INDEX CONCURRENTLY fails (invalid).", env_var="RETRIES")
parser.add_argument("--dry-run", dest="dryrun", action="store_true", help="dry run - print SQL statements only", env_var="DRY_RUN")
parser.add_argument("--pause", dest="pause_time", type=int, default=5, help="seconds to pause between reindexing. Default is 5.", env_var="PAUSE")
parser.add_argument("--print-timestamps", action="store_true", dest="print_timestamps", env_var="PRINT_TIMESTAMPS")
parser.add_argument("-l", "--log", dest="logfile")
parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", env_var="VERBOSE")
parser.add_argument("--debug", action="store_true", dest="debug", env_var="DEBUG")

args = parser.parse_args()


# basic functions
def debug_print(some_message):
    if args.debug:
        _print(some_message, "DEBUG  ")


def verbose_print(some_message):
    if args.verbose:
        _print(some_message, "VERBOSE")


def _print(some_message, mode="INFO   "):
    if args.print_timestamps:
        print("{m} {t}: {mm}".format(m=mode, t=datetime.datetime.now().isoformat(), mm=some_message))
    else:
        print(some_message)
    sys.stdout.flush()
    return True


def index_bloat_stats(size_before, size_after):
    return "(%s -> %s) := %5.2f%% | storage size usage reduced by %s to %5.2f%%" % (
        humanfriendly.format_size(size_before, True),
        humanfriendly.format_size(size_after, True),
        (100.0 * (float(size_before) / float(size_after) - 1.0)) if size_after != 0 else 0,
        humanfriendly.format_size(size_before - size_after, True),
        (100.0 * (float(size_after) / float(size_before))) if size_before != 0 else 100
    )


def dbconnect(dbname, dbuser, dbhost, dbport, dbpass):
    if dbname:
        connect_string = "dbname=%s application_name=reindex_concurrently" % dbname
    else:
        _print("A target database is required.", "ERROR  ")
        return None

    if dbhost:
        connect_string += " host=%s " % dbhost

    if dbuser:
        connect_string += " user=%s " % dbuser

    if dbpass:
        connect_string += " password=%s " % dbpass

    if dbport:
        connect_string += " port=%s " % dbport

    conn = psycopg2.connect(connect_string)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    return conn


def signal_handler(signal, frame):
    _print('exiting due to user interrupt')
    if conn:
        try:
            conn.close()
        except:
            verbose_print('could not clean up db connections')

    sys.exit(0)


def dbquery(cur, querystring, ondryrun=False):
    if not args.dryrun or ondryrun:
        debug_print("SQL: " + querystring)
        try:
            cur.execute(querystring)
        except QueryCanceledError:
            _print("Query cancelled due to enforced timeout", "NOTE   ")
        except psycopg2._psycopg.Error as err:
            _print("DB query error: %s" % err, "ERROR  ")
        except:
            _print("Unexpected error executing query: %s" % sys.exc_info()[0])
            raise
    else:
        _print("SQL: " + querystring, "DRY-RUN")


# startup debugging info

debug_print("python version: %s" % sys.version)
debug_print("psycopg2 version: %s" % psycopg2.__version__)
debug_print("parameters: %s" % repr(args))

# process arguments that argparse can't handle completely on its own

# set times
start_time = time.time()
halt_time = start_time + (args.run_min * 60)

# get set for user interrupt
conn = None
time_exit = None
signal.signal(signal.SIGINT, signal_handler)

# start logging to log file, if used
if args.logfile:
    try:
        sys.stdout = open(args.logfile, 'a')
    except Exception as ex:
        _print('could not open logfile: %s' % str(ex))
        sys.exit(1)

    _print('')
    _print('=' * 40)
    _print('reindex concurrently started %s' % str(datetime.datetime.now()))
    verbose_print('arguments: %s' % str(args))

# tableslist (csv -> array)
tablelist = args.tables.split(',') if args.tables is not None else []

# indexes (csv -> array)
indexlist = args.indexes.split(',') if args.indexes is not None else []

# ignored (csv -> array)
ignorelist = args.ignored.split(',') if args.ignored is not None else []


# globals
time_exit = False
tabcount = 0
idxcount = 0
idxcount_success = 0
idxcount_ignored = 0
idxcount_retries = 0
idxcount_notfound = 0
timeout_secs = 0
total_idx_size_before = 0
total_idx_size_after = 0


# business logic
def process_index(idx_name):
    global time_exit
    global idxcount
    global idxcount_success
    global idxcount_retries
    global idxcount_ignored
    global idxcount_notfound
    global total_idx_size_before
    global total_idx_size_after

    if time.time() >= halt_time:
        verbose_print("Reached time limit. Exiting.")
        time_exit = True
        return

    _print("Working on index %s" % idx_name)

    idxcount += 1

    if idx_name in ignorelist:
        _print("Skipping (index in 'ignore-indexes' list)")
        idxcount_ignored += 1
        return

    # figure out statement_timeout
    if args.enforcetime:
        timeout_secs = int(
            halt_time - time.time()) + 30  # note: graceful period of 30s to definitely cleanup abandoned invalid CREATE INDEX CONCURRENTLY, ..
        dbquery(cur, "SET statement_timeout = '%ss'" % timeout_secs)

    # fetch and extract details
    idx_info_query = """
        SELECT i.tablename, ix.indisprimary
        FROM pg_class c
        INNER JOIN pg_indexes i ON c.relname = i.indexname
        INNER JOIN pg_index ix ON c.oid = ix.indexrelid
        WHERE c.relname = '%s'
    """ % idx_name

    dbquery(cur, idx_info_query, True)
    idx_info = cur.fetchall()

    if len(idx_info) != 1:
        _print("Index does not exist, omit", "WARN   ")
        idxcount_notfound += 1
        return

    table = idx_info[0][0]
    is_pk = idx_info[0][1]
    debug_print("idx table: %s, is_pk: %s" % (table, is_pk))

    idx_name_new = idx_name + "_new"

    idx_create_new_query = """
        SELECT 
          indexdef, 
          regexp_replace(indexdef, '(.*)INDEX (.*) ON(.*)', '\\1INDEX CONCURRENTLY \\2_new ON\\3') AS indexdef_new
        FROM pg_indexes WHERE indexname = '%s'
    """ % idx_name

    dbquery(cur, idx_create_new_query, True)
    idx_create_new = cur.fetchall()[0][1]

    for attempt in range(1, args.retries + 1):

        if time.time() >= halt_time:
            verbose_print("Reached time limit. Exiting.")
            time_exit = True
            break

        verbose_print("Attempt #%d to create index concurrently" % attempt)

        dbquery(cur, "DROP INDEX CONCURRENTLY IF EXISTS %s" % idx_name_new)

        dbquery(cur, idx_create_new)

        # check if valid
        idx_valid_query = """
            SELECT relname, indisvalid
            FROM pg_class INNER JOIN pg_index ON pg_index.indexrelid = pg_class.oid
            WHERE relname='%s'
        """ % idx_name_new

        dbquery(cur, idx_valid_query, True)
        idx_valid = args.dryrun or cur.fetchall()[0][1]  # true on dry-run

        if idx_valid:
            verbose_print("Valid replacement index has been created, replacing existing.")

            # get reindex stats
            idx_size_before_query = "SELECT pg_relation_size('%s')" % idx_name
            dbquery(cur, idx_size_before_query, True)
            idx_size_before = cur.fetchall()[0][0]
            total_idx_size_before += idx_size_before

            idx_size_after_query = "SELECT pg_relation_size('%s')" % idx_name_new
            dbquery(cur, idx_size_after_query)
            idx_size_after = cur.fetchall()[0][0] if not args.dryrun else idx_size_before  # default on dry-run
            total_idx_size_after += idx_size_after

            verbose_print("Index bloat reduced: %s" % index_bloat_stats(idx_size_before, idx_size_after))

            if is_pk:
                debug_print("is valid, is pk, drop constraint, rename & add as new pk")
                dbquery(cur, "ANALYSE %s" % table)
                dbquery(cur, "BEGIN")
                dbquery(cur, "ALTER TABLE %s DROP CONSTRAINT %s" % (table, idx_name))
                dbquery(cur, "ALTER INDEX %s RENAME TO %s" % (idx_name_new, idx_name))
                dbquery(cur, "ALTER TABLE %s ADD PRIMARY KEY USING INDEX %s" % (table, idx_name))
                dbquery(cur, "COMMIT")
            else:
                debug_print("is valid, drop old and rename")
                dbquery(cur, "ANALYSE %s" % table)
                dbquery(cur, "DROP INDEX CONCURRENTLY %s" % idx_name)
                dbquery(cur, "ALTER INDEX %s RENAME TO %s" % (idx_name_new, idx_name))

            dbquery(cur, "ANALYSE %s" % table)

            if (args.pause_time > 0):
                verbose_print("Completed, sleeping for %ds." % args.pause_time)
                time.sleep(args.pause_time)

            idxcount_success += 1
            break
        else:
            verbose_print("Invalid replacement index, cleaning up.")
            debug_print("invalid, drop and continue")

            idx_drop_new_query = "DROP INDEX CONCURRENTLY %s" % idx_name_new
            dbquery(cur, idx_drop_new_query)

            if (args.pause_time > 0):
                verbose_print("Completed, sleeping for %ds." % args.pause_time)
                time.sleep(args.pause_time)

            idxcount_retries += 1
            continue


def process_table(table):
    global tabcount
    global time_exit

    if time.time() >= halt_time:
        verbose_print("Reached time limit. Exiting.")
        time_exit = True
        return

    _print("Working on table {0}".format(table))
    tabcount += 1

    idx_names_query = "SELECT indexname FROM pg_indexes WHERE tablename = '%s' ORDER BY indexname" % table

    dbquery(cur, idx_names_query, True)
    idx_names = cur.fetchall()

    debug_print("idx_names: {s}".format(s=idx_names))

    for idx in idx_names:
        process_index(idx[0])


# BEGIN
_print("Reindex Concurrently run starting")
verbose_print("Processing in database %s, %d tables, %d indexes, %d ignored" % (args.db, len(tablelist), len(indexlist), len(ignorelist)))

verbose_print("Connecting to database %s" % args.db)
conn = None
try:
    conn = dbconnect(args.db, args.dbuser, args.dbhost, args.dbport, args.dbpass)
except Exception as err:
    _print("error connecting to database %s (%s)" % (args.db, str(err)))
    exit()

cur = conn.cursor()

# for each index in list
for idx in indexlist:
    process_index(idx)

# for each table in list
for table in tablelist:
    process_table(table)


conn.close()


seconds_elapsed = time.time() - start_time
_print("Reindex Concurrently run completed.")
_print("Took %s" % humanfriendly.format_timespan(seconds_elapsed))
_print("Total index bloat reduced: %s" % index_bloat_stats(total_idx_size_before, total_idx_size_after))

# did we get through all tables?
# exit, report results
if idxcount == 0:
    _print("No indexes were found to reindex")
elif time_exit:
    _print("Reindexing interrupted early (configured timeout %smin)" % args.run_min)
else:
    _print("All indexes/tables reindexed")

verbose_print("...%d/%d indexes reindexed" % (idxcount_success, idxcount))
verbose_print("...%d tables processed" % tabcount)
verbose_print("...%d reindex retries due to state `invalid` after creating 'concurrently'" % idxcount_retries)
verbose_print("...%d indexes skipped cause ignored" % idxcount_ignored)
verbose_print("...%d indexes omitted cause not found" % idxcount_notfound)

sys.exit(0)
