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

    camid = data['camid'] if 'camid' in data else None

    graph_values = api.day_graph(date=data['date'], camid=camid)
    if not graph_values:
        resp = jsonify({'status': False})
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
        resp = jsonify({'status': False})
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
    camid = data['camid'] if 'camid' in data else None
    date = data['date'] if 'date' in data else None
    total_trash = api.trash_count(camid=camid, date=date)

    return jsonify(total_trash)


@app.route('/max_trash_hour', methods=['POST'])
def max_trash_hour():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp

    camid = data['camid'] if 'camid' in data else None
    trash_hour = api.max_trash_hours(camid=camid)

    return jsonify(trash_hour)


@app.route('/max_trash_day', methods=['POST'])
def max_trash_day():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp

    camid = data['camid'] if 'camid' in data else None
    trash_day = api.max_trash_days(camid=camid)

    return jsonify(trash_day)


@app.route('/max_trash_month', methods=['POST'])
def max_trash_month():
    try:
        data = json.loads(request.data)
    except ValueError:
        resp = jsonify({'status': False})
        resp.status_code = 400
        return resp

    camid = data['camid'] if 'camid' in data else None
    trash_month = api.max_trash_month(camid=camid)

    return jsonify(trash_month)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=False)