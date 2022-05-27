from influxdb import InfluxDBClient
from datetime  import datetime


client = InfluxDBClient(host='localhost',
                        port=8086,
                        username='admin',
                        password='12345',
                        database='mydb')
client.create_database('mydb')
client.get_list_database()
client.switch_database('mydb')
client.switch_database('my_new_db')

# setup payload
json_payload = []
data = {
    'measurement': 'stocks',
    'tags': {
        'ticker': 'TESLA'
    },
    'time': datetime.now(),
    'fields': {
        'open': 688.37,
        'close': 667.93
    }
}
json_payload.append(data)

# send payload
client.write_points(json_payload)

# get data from db
result = client.query('select * from stocks;')
