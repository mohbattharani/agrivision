import ftplib
from typing import Optional
import os
import time

import schedule

import cfg


class SyncData:
    def __init__(self, local_dir: Optional[str] = './camfeed', server_dir: Optional[str] = '/camfeed'):
        self.local_dir = local_dir
        self.server_dir = server_dir

    def server_sync(self):
        sess = ftplib.FTP(cfg.ftp_server.get('address'))
        sess.login(cfg.ftp_server.get('username'), cfg.ftp_server.get('password'))
        for cam in cfg.cam_info.keys():
            sess.cwd(os.path.join(self.server_dir, cam))
            for folder in sess.nlst():
                sess.cwd(os.path.join(self.server_dir, cam, folder))
                try:
                    local_files = os.listdir(os.path.join(self.local_dir, cam, folder))
                except FileNotFoundError:
                    local_files = []
                    os.mkdir(os.path.join(self.local_dir, cam))
                    os.mkdir(os.path.join(self.local_dir, cam, folder))

                server_files = [k for k in sess.nlst() if '.jpg' in k]
                file_difference = sorted(list(set(server_files) - set(local_files)))
                if file_difference:
                    for file in file_difference:
                        with open(os.path.join(self.local_dir, cam, folder, file), 'wb') as ftpfile:
                            sess.retrbinary('RETR' + file, ftpfile.write)

    def update_24(self):

        schedule.every().day.at('00:01').do(self.server_sync)
        print('Syncing Files')
        schedule.run_all()
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    ftp_sync = SyncData()
    ftp_sync.update_24()
