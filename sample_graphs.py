import json
from datetime import datetime, timedelta
import requests
import numpy as np
import pandas as pd


def get_data(url, start_date, end_date, cam_id=None, date_format='%Y-%m-%d'):
    days_diff = abs((datetime.strptime(start_date, date_format) - datetime.strptime(end_date, date_format)).days)
    dates = [(datetime.strptime(start_date, date_format) + timedelta(inc)).strftime('%Y-%m-%d')
             for inc in range(0, days_diff + 1)]

    date_list = list()
    time_list = list()
    trash_list = list()
    for date in dates:
        if cam_id is None:
            req = {"date": date}
        else:
            req = {"date": date, "camid": cam_id}
        
        req = json.dumps(req)
        x = requests.post(url, data=req)
        if x.text['status'] is False:
            continue
        times, trash_count = zip(*json.loads(x.text).items())
        trash_count = list(map(int, trash_count))
        date_list.extend([date for i in range(len(times))])
        time_list.extend(times)
        trash_list.extend(trash_count)

    n = len(date_list)
    date_list, time_list, trash_list = np.asarray(date_list).reshape(n, 1), np.asarray(time_list).reshape(n, 1),\
                                       np.asarray(trash_list).reshape(n, 1)
    final_array = np.concatenate([date_list, time_list, trash_list])
    return final_array


if __name__ == '__main__':
    day_url = 'http://0.0.0.0:5000/day_graph'
    array_list = get_data(day_url, start_date='2020-05-10', end_date='2020-06-03')

    df = pd.DataFrame(data=array_list, index=['Date', 'Time', 'Count'])
    df.to_csv('data.csv', index=False)
