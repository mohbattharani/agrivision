cam_ids = ['lums', 'test1', 'test2'] #update this when adding new cam
mongo_cfg = {'db_server': {'host': 'mongo', 'port': '27017'}, 'db_name': 'trash', 'db_raw_clc': 'main', 'db_manual_clc': 'manual'}
directories = {'main_dir': 'camfeed', 'save_dir': 'results'}
# camid: {longitude,latitude,description}
cam_info = {'lums': {'longitude': 31.472780, 'latitude': 74.409863, 'description': 'Roohi Nala'},
            'test1': {'longitude': 31.472780, 'latitude': 74.409863, 'description': 'Roohi Nala 1'},
            'test2': {'longitude': 31.472780, 'latitude': 74.409863, 'description': 'Roohi Nala 2'}}