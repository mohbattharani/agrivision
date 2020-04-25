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
        resp.status_code = 400
        return resp
    return jsonify(graph_values)


@app.route('/range_graph', methods=['POST'])
def range_graph():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp
    if 'start_date' and 'end_date' not in data:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp
    if 'camid' not in data:
        graph_values = api.range_graph(start_date=data['start_date'], end_date=data['end_date'])
    else:
        graph_values = api.range_graph(start_date=data['start_date'], end_date=data['end_date'], camid=data['camid'])
    if not graph_values:
        resp = jsonify({'status': 'Data does not exist'})
        resp.status_code = 400
        return resp
    return jsonify(graph_values)


@app.route('/total_trash', methods=['POST'])
def trash_count():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp
    if 'camid' not in data:
        total_trash = api.trash_count()
    else:
        total_trash = api.trash_count(camid=data['camid'])
    return jsonify(total_trash)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)