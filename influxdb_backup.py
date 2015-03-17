#!/usr/bin/env python
"""Influxdb Backup.

Usage:
  influxdb_backup.py backup [-c FILE] [-i INTERVAL | -f ] [-o] [-t DIR] [-w WORKERS]
  influxdb_backup.py (-h | --help)
  influxdb_backup.py --version

Options:
  -h --help                 Show this screen.
  -v --version              Show version.
  -c --config=<config>      Configuration file [default: ~/.influxdb_backup.yaml].
  -i --incremental=<incr>   Time interval to backup into (ie 8h, 1d, 6m) [default: 1d].
  -f --full                 Do full backup.
  -o --overwrite            Overwrite backup files.
  -t --target=<dir>         Target directory to backup to [default: .]
  -w --workers=<workers>    Number of workers downloading json data (incremental only) [default: 10]
"""

from docopt import docopt
import yaml
import requests
from requests.adapters import HTTPAdapter
import dateutil.relativedelta
import os
import sys
import re
import datetime
import json
from multiprocessing import Pool
import time

INCR_MAP = {
    "m": "minutes",
    "h": "hourly",
    "d": "daily",
    "M": "monthly"
}

def get_dbs(conf):
    print "Getting list of databases..."
    response = requests.get('%s:%d/db' % (conf['host'], conf['port']), auth=(conf['username'], conf['password']))
    if 'db_regex' in conf:
        reg = re.compile(conf['db_regex'])
        dbs = [d['name'] for d in response.json() if reg.match(d['name'])]
    else:
        dbs = [d['name'] for d in response.json()]
    print "done."
    return dbs

def backup(start_date, path, url, auth, params):
    session = requests.Session()
    retries=5
    if url.startswith('https'):
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
    else:
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    try:
        print "Pulling: %s" % start_date
        response = session.get(url, auth=auth, params=params, stream=True)
        response.raise_for_status()
        print "Writing: %s.json (%s)" % (start_date.strftime('%s'), start_date)
        with open(path, 'w') as j:
            for chunk in response.iter_content():
                # if chuck has data, write to file.
                if chunk:
                    # We ensure each complete json text is written to its own line. 
                    # This makes it easy to restore these files in a scalable wasy
                    # via xreadlines()
                    if chunk.endswith("}"):
                        j.write(chunk + "\n")
                    else:
                        j.write(chunk)
    except Exception as ex:
        print "INFLUXDB REQUEST FAILED"
        print sys.exc_info()
        if os.path.isfile(path):
            os.remove(path)

def pre_process_backup(db, path, conf, chunked=True):
    pool = Pool(int(args['--workers']))
    results = []
    auth=(conf['username'], conf['password'])
    url='%s:%d/db/%s/series' % (conf['host'], conf['port'], db)
    if args['--full']:
        params={
            'q': "select * from %s" % (conf['table_regex']),
            'chunked': str(chunked).lower()
        }
        working_dir = '%s/%s/full' % (path, db)
        if not os.path.isdir(working_dir):
            os.makedirs(working_dir)
        d = datetime.datetime.now()
        json_file = "%s/%s.json" % (working_dir, d.strftime('%s'))
        backup(d, json_file, url, auth, params)
    else:
        count = int(re.sub('[^0-9]', '', args['--incremental']))
        interval = INCR_MAP[args['--incremental'][-1]] or None
        if args['--incremental'][-1] == 'm':
            end_date = datetime.datetime.now().replace(second=0, microsecond=0)
        elif args['--incremental'][-1] == 'h':
            end_date = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
        elif args['--incremental'][-1] == 'd':
            end_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        elif args['--incremental'][-1] == 'M':
            end_date = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        working_dir = '%s/%s/%s' % (path, db, interval)
        if not os.path.isdir(working_dir):
            os.makedirs(working_dir)
        for _ in range(count):
            if args['--incremental'][-1] == 'm':
                start_date = end_date - dateutil.relativedelta.relativedelta(minutes=1)
            elif args['--incremental'][-1] == 'h':
                start_date = end_date - dateutil.relativedelta.relativedelta(hours=1)
            elif args['--incremental'][-1] == 'd':
                start_date = end_date - dateutil.relativedelta.relativedelta(days=1)
            elif args['--incremental'][-1] == 'M':
                start_date = end_date - dateutil.relativedelta.relativedelta(months=1)
            params={
                'chunked': str(chunked).lower(),
                'q': "select * from %s where time > %ss and time < %ss" % (conf['table_regex'],
                                                                            start_date.strftime('%s'),
                                                                            end_date.strftime('%s'))
            }
            json_file = "%s/%s.json" % (working_dir, start_date.strftime('%s'))
            if args['--overwrite'] or not os.path.isfile(json_file):
                results.append(pool.apply_async(backup, [start_date, json_file, url, auth, params]))
            end_date = start_date
        # wait for all threads to finish downloading
        while sum(1 for x in results if not x.ready()) > 0:
            time.sleep(30)

if __name__ == '__main__':
    args = docopt(__doc__, version='Influxdb Backup 0.1')
    
    ### Validate incremental input ###
    if args['--incremental'][-1] not in INCR_MAP.keys():
        print "--incremental must end with 'm' (minutes), 'h' (hourly), 'd' (daily), 'M' (monthly)"
        sys.exit(1)
    try:
        int(re.sub('[^0-9]', '', args['--incremental']))
    except ValueError:
        print "--incremental must start with a number"
        sys.exit(1)
    ######

    ### Load config file ###
    # set default config file is none given
    if not args['--config']:
        args['--config'] = '~/.influxdb_backup.yaml'
    # check if config file exists
    if not os.path.isfile(args['--config']):
        print "Could not file configuration file. Use `-c` or create config at `~/.influxdb_backup.yaml`"
        sys.exit(1)
    # load config file
    with open(args['--config']) as f:
        config = yaml.safe_load(f)
    ######

    ### backup influxdb ###
    if args['backup']:
        for name, conf in config.iteritems():
            print "Backing up: %s" % name
            path = '%s/%s/' % (args['--target'], name)

            # create directory if it doesn't exist
            if not os.path.isdir(path):
                os.mkdir(path)

            # get list of dbs to backup
            dbs = get_dbs(conf)
            for db in dbs:
                print "Working Database: %s" % db
                pre_process_backup(db, path, conf)
        print "done."
    ######
