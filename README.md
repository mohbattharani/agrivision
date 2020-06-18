# agrobotics-deployment

#### Quick start
Download the weights from https://drive.google.com/open?id=1YDSFmdp2MhWVpXq8sJWGbMOU_U0OBL2O and place it inside the model/logs folder. </br>

If you want to start from a specific date please specify starting date in db_script.py and gsheets.py. </br>

Go to the main directory and run docker-compose up

#### Sample post command for api
curl -H "Content-Type: application/json" -X POST -d '{"start_date":"2020-04-26", "end_date":"2020-04-28",  "camid": "lums2"}' http://localhost:5050/range_graph 
