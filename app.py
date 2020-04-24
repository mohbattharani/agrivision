import json
from flask import Flask, request, jsonify
from api_calls import ApiCall
import cfg

app = Flask(__name__)
api = ApiCall(cfg.mongo_cfg.get('db_name'), cfg.mongo_cfg.get('db_raw_clc'))


@app.route('/day_graph', methods=['POST'])
def day_graph():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp
    if 'date' not in data:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp
    if 'camid' not in data:
        graph_values = api.day_graph(date=data['date'])
    else:
        graph_values = api.day_graph(date=data['date'], camid=data['camid'])
    if not graph_values:
        resp = jsonify({'status': 'Data does not exist'})
        resp.status_code = 404
        return resp
    return jsonify(graph_values)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)