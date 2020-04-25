# agrobotics-deployment

Download the weights from https://drive.google.com/open?id=1YDSFmdp2MhWVpXq8sJWGbMOU_U0OBL2O and place it inside the model/logs folder.

#### Sample post command for api
curl -H "Content-Type: application/json" -X POST -d '{"start_date":"2020-04-26", "end_date":"2020-04-28",  "camid": "lums2"}' http://localhost:5050/range_graph 