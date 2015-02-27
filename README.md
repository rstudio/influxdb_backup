influxdb_backup
===============

A cli to backing up influxdb into json files.


```
Usage:
  influxdb_backup.py backup [-c FILE] [-i INTERVAL | -f ] [-o] [-t DIR] [-w WORKERS]
  influxdb_backup.py (-h | --help)
  influxdb_backup.py --version

Options:
  -h --help                 Show this screen.
  -v --version              Show version.
  -v --config=<config>      Configuration file [default: ~/.influxdb_backup.yaml].
  -i --incremental=<incr>   Time interval to backup into (ie 8h, 1d, 6m) [default: 1d].
  -f --full                 Do full backup.
  -o --overwrite            Overwrite backup files.
  -t --target=<dir>         Target directory to backup to [default: .]
  -w --workers=<workers>    Number of workers downloading json data (incremental only) [default: 10]
```
