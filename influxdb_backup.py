#!/usr/bin/env python
"""Influxdb Backup.

Usage:
  influxdb_backup.py backup [-c FILE] [-i INTERVAL | -f ] [-o] [-t DIR] [-w WORKERS]
  influxdb_backup.py restore [-c FILE] [-w WORKERS]
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
import hashlib
import traceback
from multiprocessing import Pool
import time

INCR_MAP = {
    "m": "minutes",
    "h": "hourly",
    "d": "daily",
    "M": "monthly"
}

# collect stdin (if any)
INPUT = [] if sys.stdin.isatty() else [y.strip() for y in sys.stdin.readlines()]


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


def backup(start_date, json_path, checksum_path, url, auth, params):
    def remove_files():
        for p in [json_path, checksum_path]:
            if os.path.isfile(p):
                os.remove(path)

    session = requests.Session()
    retries = 5
    checksum = hashlib.sha1()
    if url.startswith('https'):
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
    else:
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    try:
        print "Pulling: %s" % start_date
        response = session.get(url, auth=auth, params=params, stream=True)
        print "Writing: %s.json (%s)" % (start_date.strftime('%s'), start_date)
        with open(json_path, 'w') as j:
            for chunk in response.iter_content():
                # if chuck has data, write to file.
                if chunk:
                    # We ensure each complete json text is written to its own line. 
                    # This makes it easy to restore these files in a scalable wasy
                    # via xreadlines()
                    if chunk.endswith("}"):
                        chunk += "\n"
                    j.write(chunk)
                    checksum.update(chunk)
            j.flush()
        response.raise_for_status()
        print "Writing checksum: %s.json.sha1 (%s)" % (start_date.strftime('%s'), start_date)
        with open(checksum_path, 'w') as f:
            f.write(checksum.hexdigest() + "  " + json_path.split('/')[-1])

        # check that the checksums actually match
        if not _check_checksum(checksum_path):
            remove_files()
    except Exception as ex:
        print traceback.print_exc()
        remove_files()
        raise ex


def _get_end_date():
    if args['--incremental'][-1] == 'm':
        return datetime.datetime.now().replace(second=0, microsecond=0)
    elif args['--incremental'][-1] == 'h':
        return datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
    elif args['--incremental'][-1] == 'd':
        return datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    elif args['--incremental'][-1] == 'M':
        return datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _get_start_date(end_date):
    if args['--incremental'][-1] == 'm':
        return end_date - dateutil.relativedelta.relativedelta(minutes=1)
    elif args['--incremental'][-1] == 'h':
        return end_date - dateutil.relativedelta.relativedelta(hours=1)
    elif args['--incremental'][-1] == 'd':
        return end_date - dateutil.relativedelta.relativedelta(days=1)
    elif args['--incremental'][-1] == 'M':
        return end_date - dateutil.relativedelta.relativedelta(months=1)


def _check_checksum(checksum_path):
    # read checksum file to get expected checksum value
    with open(checksum_path, 'r') as f:
        expected_checksum, file_name = f.read().split()

    # get checksum of the json file
    real_checksum = hashlib.sha1()
    with open(os.path.dirname(os.path.realpath(checksum_path)) + "/" + file_name, 'r') as f:
        # since we want to avoid reading the entire file to memory, we'll use xreadlines to calc
        # the checksum line by line
        for x in f.xreadlines():
            real_checksum.update(x)

    return expected_checksum == real_checksum.hexdigest()


def pre_process_backup(db, path, conf, chunked=True):
    pool = Pool(int(args['--workers']))
    results = []
    auth = (conf['username'], conf['password'])
    url = '%s:%d/db/%s/series' % (conf['host'], conf['port'], db)
    if args['--full']:
        params = {
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
        end_date = _get_end_date()
        working_dir = '%s/%s/%s' % (path, db, interval)
        if not os.path.isdir(working_dir):
            os.makedirs(working_dir)
        for _ in range(count):
            start_date = _get_start_date(end_date)
            params = {
                'chunked': str(chunked).lower(),
                'q': "select * from %s where time > %ss and time < %ss" % (conf['table_regex'],
                                                                           start_date.strftime('%s'),
                                                                           end_date.strftime('%s'))
            }
            json_file = "%s/%s.json" % (working_dir, start_date.strftime('%s'))
            checksum_file = json_file + ".sha1"
            if args['--overwrite'] or not os.path.isfile(json_file) or not os.path.isfile(
                    checksum_file) or not _check_checksum(checksum_file):
                results.append(pool.apply_async(backup, [start_date, json_file, checksum_file, url, auth, params]))
            end_date = start_date
        # wait for all threads to finish downloading
        while sum(1 for x in results if not x.ready()) > 0:
            time.sleep(30)


def restore(file_path, db_name, conf):
    auth = (conf['username'], conf['password'])
    url = '%s:%d/db/%s/series' % (conf['host'], conf['port'], db_name)
    session = requests.Session()
    retries = 5
    if url.startswith('https'):
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retries))
    else:
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retries))
    try:
        print "Starting: %s" % file_path
        with open(file_path, 'r') as f:
            for json_string in f.xreadlines():
                response = session.post(url, auth=auth, data=("[%s]" % json_string))
                response.raise_for_status()
    except Exception as ex:
        print "INFLUXDB REQUEST FAILED"
        print sys.exc_info()


def _load_config():
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
        return yaml.safe_load(f)
        ######


if __name__ == '__main__':
    args = docopt(__doc__, version='Influxdb Backup 0.1')

    config = _load_config()

    ### backup/restore influxdb ###
    if args['backup']:

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
    elif args['restore']:

        ### Validate input given
        if not INPUT:
            print "Must pipe in (as stdin) a list of files to restore (ie use `ls -1` or `find` commands)"
            sys.exit(1)
        else:
            for i in INPUT:
                try:
                    config_name, db_name, _ = i.split('/', 2)
                except ValueError as ex:
                    print "Restore files must contain relative path to backup root directory"
                if config_name not in config:
                    print "First element (%s) in file path (%s) does not exist as config in configuration file." % (
                        config_name, i)
        ####

        pool = Pool(int(args['--workers']))
        results = []
        for i in INPUT:
            config_name, db_name, _ = i.split('/', 2)
            conf = config[config_name]
            results.append(pool.apply_async(restore, [i, db_name, conf]))
        # wait for all threads to finish downloading
        while sum(1 for x in results if not x.ready()) > 0:
            time.sleep(10)
            ######
