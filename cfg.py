mongo_cfg = {'db_server': {'host': 'mongo', 'port': '27017'}, 'db_name': 'trash', 'db_raw_clc': 'main', 'db_manual_clc': 'manual'}
directories = {'main_dir': 'camfeed', 'save_dir': 'results'}
ftp_server = {'address': '10.99.0.66', 'username': 'lums\\abdul_amin', 'password': 'Lums12345'}
# camid: {longitude,latitude,description}
# Don't use underscore in camera id
cam_info = {'LUMS': {'longitude': 74.409863, 'latitude': 31.472780, 'description': 'Rohi Nala-xx'}}
            # 'lums2': {'longitude': 74.413239, 'latitude': 31.478238, 'description': 'Rohi Nala-xx'},
            # 'comsats': {'longitude': 74.211902, 'latitude': 31.398038, 'description': 'Hudiara Drain-xx'}}
            # 'test1': {'longitude': 31.472780, 'latitude': 74.409863, 'description': 'Roohi Nala 1'},
            # 'test2': {'longitude': 31.472780, 'latitude': 74.409863, 'description': 'Roohi Nala 2'}}
api_urls = {'range_data': 'http://flask_app:5000/range_graph',
            'day_time_data': 'http://flask_app:5000/day_graph',
            'total_trash': 'http://flask_app:5000/total_trash'}
