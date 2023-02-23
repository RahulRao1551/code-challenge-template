from flask import abort, Flask, request, jsonify

from utils import generate_where_clause, get_data

app = Flask(__name__)

WX_SCHEMA = 'wx_schema'
YLD_SCHEMA = 'yld_schema'
WX_TABLE = 'wx_data'
YLD_TABLE = 'yld_data'
AVG_TABLE = 'avg_table'


@app.route('/')
def index():
    return jsonify({'Greeting': 'Hello Corteva!'})


@app.route('/api/weather', methods=['GET'])
def get_weather():
    """
    Gets paginated weather from wx_table
    Returns: JSON object
    """
    args = request.args
    param_1_tup = ('station_id', args.get('station_id'))
    param_2_tup = ('date', args.get('date'))
    offset = args.get('offset', 0)
    limit = args.get('limit', 10)
    bounds_dict = {'offset':offset,'limit':limit}

    where_clause = generate_where_clause([param_1_tup, param_2_tup])
    pag_dict = get_payload(WX_SCHEMA, WX_TABLE, where_clause, bounds_dict)

    return jsonify({"success": True, "payload": pag_dict})


@app.route('/api/yield', methods=['GET'])
def get_yield():
    """
    Gets paginated grain yield from yld_table
    Returns: JSON object
    """
    args = request.args
    param_1_tup = ('year', args.get('year'))
    offset = args.get('offset', 0)
    limit = args.get('limit', 10)
    bounds_dict = {'offset': offset, 'limit': limit}

    where_clause = generate_where_clause([param_1_tup])
    pag_dict = get_payload(YLD_SCHEMA, YLD_TABLE, where_clause, bounds_dict)

    return jsonify({"success": True, "payload": pag_dict})


@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    """
    Gets paginated weather stats from avg_table
    Returns: JSON object
    """
    args = request.args
    param_1_tup = ('station_id', args.get('station_id'))
    param_2_tup = ('year', args.get('year'))
    offset = args.get('offset', 0)
    limit = args.get('limit', 10)
    bounds_dict = {'offset':offset, 'limit':limit}

    where_clause = generate_where_clause([param_1_tup, param_2_tup])
    pag_dict = get_payload(WX_SCHEMA, AVG_TABLE, where_clause, bounds_dict)

    return jsonify({"success": True, "payload": pag_dict})


def get_payload(schema, table, where_clause, bounds_dict):
    """
    Preprocesses pagination structure, queries data
    Returns: String
    """
    pagination_clause = f" LIMIT {bounds_dict['limit']} OFFSET {bounds_dict['offset']}"
    ans = get_data(schema, table, where_clause, pagination_clause)
    count_str = get_data(schema, table, where_clause, '', True)
    count = count_str[0]['count']
    bounds_dict['count'] = count

    url = request.base_url
    # get only non-pagination query params
    query = request.query_string.decode()
    head = query.partition('offset')[0]
    if len(head) > 0 and head[-1] == '&':
        query = head[:-1]
    query = head

    return format_pagination(ans,url,query,bounds_dict)


def format_pagination(results, url, query, bounds):
    """
    Generates pagination structure
    Returns: String
    """
    offset, limit, count = int(bounds['offset']), int(bounds['limit']), int(bounds['count'])
    limit_copy = offset - 1
    query = query + '&' if len(query) == 0 or query[-1] != '&' else query
    # ensure valid pagination params
    if count < offset or limit < 0:
        abort(404)

    pag_params = {'offset': offset, 'limit': limit, 'count': count, 'results': results}
    # generate url for previous offset,limit
    if offset == 0:
        pag_params['previous'] = str()
    else:
        offset_copy = max(0, offset - limit)
        pag_params['previous'] = f'{url}?{query}offset={offset_copy}&limit={limit_copy}'
    # generate url for next offset,limit
    if offset + limit > count:
        pag_params['next'] = str()
    else:
        offset_copy = offset + limit
        pag_params['next'] = f'{url}?{query}offset={offset_copy}&limit={limit}'
    return pag_params


if __name__ == '__main__':
    app.run()