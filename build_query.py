import datetime
import logging
from influxdb_client import InfluxDBClient
# from influxdb import InfluxDBClient

request = {
    "database": "cptDB",
    "date_from": "2020-05-15",  # time format with date
    "date_to": "2022-05-16T22:15:00Z",  # time format with date and time
    "last_rides": {  # [None, None],  # Number of rides; min timespan between rides in minutes
        "number_of_rides": 3,
        "time_span_between_rides": 2
    },
    "measurement": "mercedes",
    "tags": {  # Number of tags 0+
        "baujahr": {  # tag value; arithmetic comparison operator (<, =, >)
            "value": "2013",
            "operator": ">"
        },
        "farbe": {
            "value": "blau",
            "operator": "=="
        },
        "tag_3": None
    },
    "fields": {  # Number of fields 1+
        "km": {
            "value": "12000",
            "operator": ">"
        },
    }
}



request_ = {
    "database": "mydb",
    "date_from": "2021-05-09T04:10:10Z",  # time format with date
    "date_to": "2022-05-26T22:15:00Z",  # time format with date and time
    "last_rides": None,
    "measurement": "signal_1",
    "tags": {  # Number of tags 0+
        "road": {  # tag value; arithmetic comparison operator (<, =, >)
            "value": "autobahn",
            "operator": "=="
        }
    },
    "fields": None
}








def get_tables(query_in: str):

    url = "http://143.93.247.136:8086"
    # url = "http://143.93.247.135:8086"
    token = 'my-token'
    org = 'my-org'
    # mybucket = 'cptDB'

    client = InfluxDBClient(url=url, token=token, org=org)
    # client = InfluxDBClient('143.93.247.135', 8086, 'admin', '12345', 'mydb')
    # client.switch_database('mydb')


    tables = client.query_api().query(query_in)
    # tables  = client.query("select * from signal_2")

    return tables


def define_start_time_for_n_last_rides(number_of_rides: int, time_span: int, query_in: str) -> datetime.datetime:
    tables = get_tables(query_in)

    previous_time = datetime.datetime
    the_first = True
    time_delta = datetime.timedelta(minutes=time_span)

    for table in tables:
        counter = 0 # TODO: exactly define the place of initialisation of "counter"
        # print(table)
        for record in table.records:
            # print(record)
            if the_first:
                previous_time = record.get_time()
                the_first = False
                continue

            interval = previous_time - record.get_time()
            if interval > time_delta:
                counter += 1
                # print("COUNTER: {}".format(counter))
                if counter == number_of_rides:
                    #print("jaAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa {}".format(previous_time))
                    break

            # print(interval)
            # print(previous_time)
            # print(record.get_time())
            previous_time = record.get_time()

    return previous_time


def build_query(get_request: dict) -> str:
    """

    :param get_request:
    :return:
    """

    # There is two possibles cases: get last n rides or all rids in time interval
    # This exception is thrown if the both cases couldn't be described
    if not get_request['date_from']:
        logging.info('Attribute error: Key "date_from" in HTTP-GET request is None')
        raise Exception("data_from in HTTP-GET request could not be None")

    query = ''  # string variable for return value which is a query for influxDB
    # start_time = ''

    # add database name to query
    try:
        query += 'from(bucket: "' + get_request['database'] + '")'
    except AttributeError:
        # if database name wasn't passed
        logging.info('Function "build_query": database name wasn\'t found in passed parameter')
        print('Function "build_query": database name wasn\'t found in passed parameter')

    try:
        # Add start value of time interval for required data as filter criteria to query
        start_time = '|> range(start: ' + get_request['date_from']
        query += start_time

        # Add end value of time interval for required data as filter criteria to query
        if get_request['date_to']:
            query += ', stop: ' + get_request['date_to']

        query += ')'

        # Add measurement name as filter criteria to query
        if get_request['measurement']:
            query += '|> filter(fn:(r) => r._measurement == "' + get_request['measurement'] + '")'

        # Add tag values as filter criteria to query
        if get_request['tags']:
            for tag_name, tag_data in get_request['tags'].items():
                if tag_name and tag_data:
                    query += ' |> filter(fn:(r) => r.' + tag_name + ' ' + \
                             tag_data['operator'] + ' "' + tag_data['value'] + '")'

        # Add field value as filter criteria to query
        if get_request['fields']:
            # field_name = list(get_request['fields'].keys())[0]
            field_value = list(get_request['fields'].values())[0]['value']
            field_operator = list(get_request['fields'].values())[0]['operator']

            # for int values
            query += ' |> filter(fn:(r) => r._value ' + field_operator + ' ' + field_value + ')'
            # for string values
            # + field_operator + ' "' + field_value + '")'

            '''
        for multiple fields
        for field_name, field_data in get_request['fields'].items():
            if field_data:
            query += ' |> filter(fn:(r) => r._field ' \
                  + field_data['operator'] + ' "' + field_data['value'] + '")'

        
        # example from internet
        # https://community.influxdata.com/t/how-do-i-select-multiple-field-columns-with-flux/11314/3
        from(bucket: "test/autogen")
        | > range(start: MyStartTime, stop: MyEndTime)
        | > filter(fn: (r) = > r._measurement == "test" and r._field = ~ / a | b / )
        | > pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        | > filter(fn: (r) = > r.a > 10 and r.b == 30)
        '''

        # add sort statement if the number of rides isn't None
        if get_request['last_rides']:
            # temporary add descending sort for search last n rides
            string_for_descent_sort = ' |> sort(columns: ["_time"],desc: true)'
            query += string_for_descent_sort
            # create a start time to remove points which are not in n rides
            start_time_for_replacing = \
                define_start_time_for_n_last_rides(get_request['last_rides']['number_of_rides'],
                                                   get_request['last_rides']['time_span_between_rides'], query)
            # convert datetime to flux format
            # (2022-05-09 04:08:10.919937+00:00 -> 2022-05-09T04:08:10Z)
            start_time_for_replacing = str(start_time_for_replacing)[:20]
            start_time_for_replacing = start_time_for_replacing.replace(' ', 'T')
            start_time_for_replacing = start_time_for_replacing.replace('.', 'Z')
            # replace old start time in query with new one
            string_for_replacing = '|> range(start: ' + start_time_for_replacing
            query = query.replace(start_time, string_for_replacing, 1)
            # remove descending sort of output
            query = query.replace(string_for_descent_sort, '', 1)

    except AttributeError:
        # if some keys or values in passed parameter have incorrect type or missing
        logging.info('Function "build_query": some keys or values in passed parameter have incorrect type or missing')
        print('Function "build_query": some keys or values in passed parameter have incorrect type or missing')

    return query


# mybucket = 'cptDB'
query_for_rides = build_query(request)
print(query_for_rides)
tables = get_tables(query_for_rides)
print(tables)
# for table in tables:
#     print(table.records)
#     print()

obj = datetime.datetime.now()
# print(obj.timestamp())
