"""
This script is used to back up mongodb database every sunday.

This script requires the schedule library to be installed. The database server and port also need to be defined in the
configuration file.

This script can also be imported as a module and contains the following two methods:
    * dump_database - Dumps the latest mongo database from current date to the mongo folder
    * main - Creates Backup every sunday using the dump_database function
"""

import os
import subprocess
import time
from datetime import datetime

import schedule

import cfg


# Backup the database through mongodump
def dump_database():
    """
    Dumps mongo database from current date to the mongo folder
    """

    current_date = datetime.now().strftime('%Y-%m-%d')
    save_dir = os.path.join('mongo', current_date)
    # Check if directory exists
    if not os.path.exists(save_dir):
        # Create save folder
        os.mkdir(save_dir)
    # Running terminal mongodump command
    subprocess.Popen('mongodump --forceTableScan -h {}:{} --out ./mongo/{}'.format(
        cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')), current_date
    ), shell=True)


def main():
    """
    Creates Backup every sunday using the dump_database function
    """

    # schedule it after a number of days
    schedule.every().sunday.do(dump_database)
    # schedule.every(25).seconds.do(run)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
