import os
import subprocess
import time
from datetime import datetime

import schedule

import cfg


# Backup the database through mongodump
def run():
    current_date = datetime.now().strftime('%Y-%m-%d')
    save_dir = os.path.join('mongo', current_date)
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
    subprocess.Popen('mongodump --forceTableScan -h {}:{} --out ./mongo/{}'.format(
        cfg.mongo_cfg.get('db_server').get('host'), int(cfg.mongo_cfg.get('db_server').get('port')), current_date
    ), shell=True)


def main():
    # schedule it after a number of days
    schedule.every().sunday.do(run)
    # schedule.every(25).seconds.do(run)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    main()
