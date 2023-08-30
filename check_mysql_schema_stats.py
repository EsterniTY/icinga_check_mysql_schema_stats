#!/usr/bin/python3.9 -u
# -*- coding: utf-8 -*-
import argparse
import sys
import pymysql
import time
import os.path
import tempfile
import hashlib

__version__ = '1.0.0'

def read_old_data(file): 
    olddata = {}

    if not os.path.exists(file):
        return {}

    try:
        with open(file, 'r') as f:
            header = f.readline().split('|')
            now = int(time.time())

            if header[0] == __version__ and int(header[1]) < now:
                olddata['__timedelta__'] = now - int(header[1])
                for row in f:
                    clean = row.strip().split(':')
                    olddata[clean[0]] = {'fetch':  int(clean[1]), 'insert': int(clean[2]), 'update': int(clean[3]), 'delete': int(clean[4]) }
            f.close()
    except IOError as e:
        print('[CRITICAL] Error reading cache data: %s' % e.args[1])
        exit(2)

    return olddata

def read_data(args):
    data = {}

    try:
        db = pymysql.connect(host=args.hostname, user=args.username, password=args.password, db='sys', cursorclass=pymysql.cursors.DictCursor)
        c = db.cursor()

        c.execute('SELECT table_schema, table_name, rows_fetched, rows_inserted, rows_updated, rows_deleted FROM sys.schema_table_statistics where table_schema = "%s"' % args.schema)

        for row in c.fetchall():
            table = '%s.%s' % (row['table_schema'].replace(' ', '_'), row['table_name'].replace(' ', '_'))

            data[table] = {
                'fetch': int(row['rows_fetched']),
                'insert': int(row['rows_inserted']),
                'update': int(row['rows_updated']),
                'delete': int(row['rows_deleted'])
            }

        c.close()
    except pymysql.err.OperationalError as e:
        print('[CRITICAL] %s' % e.args[1])
        exit(2)

    return data

def write_data(file, data):
    try:
        with open(file, 'w') as f:
            f.write('%s|%s\n' %(__version__, int(time.time())))

            for table in data:
                f.write('%s:%s:%s:%s:%s\n' % (table, data[table]['fetch'], data[table]['insert'], data[table]['update'], data[table]['delete']))

            f.close()
    except IOError as e:
        print('[CRITICAL] Error writing cache data: %s' % e.args[1])
        exit(2)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-V', '--version', action='version', version='%(prog)s v' + sys.modules[__name__].__version__)
    parser.add_argument('-H', '--hostname', help='The host address of the MySQL server', required=True)
    parser.add_argument('-u', '--username', help='MySQL user', required=True)
    parser.add_argument('-p', '--password', help='MySQL password')
    parser.add_argument('-s', '--schema', help='MySQL schema', required=True)
    args = parser.parse_args()

    suffix = '%s:%s:%s:%s' % (os.getlogin(), args.hostname, args.username, args.schema)
    tmp_file = '%s/%s.%s.dat' % (tempfile.gettempdir(), os.path.basename(__file__), hashlib.md5(suffix.encode('utf-8')).hexdigest())

    olddata = read_old_data(tmp_file)
    data = read_data(args)
    msgdata = []
    perfdata = []

    if len(data) == 0:
        print('[CRITICAL] No data recieved')
        exit(2)

    write_data(tmp_file, data)

    if len(olddata) == 0:
        print('[UNKNOWN] Collecting data')
        exit(3)
    
    totals = {'fetch': 0, 'insert': 0, 'update': 0, 'delete': 0}

    for table in data:
        if table in olddata:
            f = int((data[table]['fetch'] - olddata[table]['fetch']) / olddata['__timedelta__'])
            i = int((data[table]['insert'] - olddata[table]['insert']) / olddata['__timedelta__'])
            u = int((data[table]['update'] - olddata[table]['update']) / olddata['__timedelta__'])
            d = int((data[table]['delete'] - olddata[table]['delete']) / olddata['__timedelta__'])
            totals['fetch'] += f
            totals['insert'] += i
            totals['update'] += u
            totals['delete'] += d

            details = []
            if f: details.append('fetched: %i' % f)
            if i: details.append('inserted: %i' % i)
            if u: details.append('updated: %i' % u)
            if d: details.append('deleted: %i' % d)

            if len(details):
                msgdata.append('\_ [%s] %s: %s' % ('OK', table, ', '.join(details)))

            perfdata.append('%s.fetch=%i;;;'  % (table, f))
            perfdata.append('%s.insert=%i;;;' % (table, i))
            perfdata.append('%s.update=%i;;;' % (table, u))
            perfdata.append('%s.delete=%i;;;' % (table, d))
        else:
            msgdata.append('[%s] %s: fetching' % ('UNKNOWN', table))

    if len(perfdata):
        perfdata.append('%s.__total.fetch=%i;;;' % (args.schema, totals['fetch']))
        perfdata.append('%s.__total.insert=%i;;;' % (args.schema, totals['insert']))
        perfdata.append('%s.__total.udpate=%i;;;' % (args.schema, totals['update']))
        perfdata.append('%s.__total.delete=%i;;;' % (args.schema, totals['delete']))

        print('[%s] %s fetches: %i, inserts: %i, updates: %i, deletes: %i within last %i seconds' % (
            'OK',
            'proxy',
            totals['fetch'],
            totals['insert'],
            totals['update'],
            totals['delete'],
            olddata['__timedelta__']
        ))
        
        print('%s|%s' % ('\n'.join(msgdata), ' '.join(perfdata)))
    else:
        print('\n'.join(msgdata))
