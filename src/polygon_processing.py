import traceback
import time
import json
import requests
import pandas as pd
import glob
import geobuf
import zstd
from shapely.geometry import shape
import psycopg2
from psycopg2 import extras
import os
import datetime

mailgunuser = 'Monitoring Power Outage'
destination = os.getenv('destination')
mailguncreds = os.getenv('mailguncreds')
maildomain = os.getenv('maildomain')
psql_creds = os.getenv('psqlcreds')


def send_email(warning):
	return requests.post(
		f"https://api.mailgun.net/v3/{maildomain}/messages",
		auth=("api", mailguncreds),
		data={"from": f"{mailgunuser} <mailgun@{maildomain}>",
			"to": [destination],
			"subject": "Monitoring alert",
			"text": f"{warning}. UTC time when something went wrong: {datetime.datetime.utcnow()}"})


def figure_out_files_to_add(cursor):
    cursor.execute('select max(timestamp) from twkboutagepolygons')
    last_time = int(cursor.fetchall()[0][0].timestamp())
    cursor.connection.commit()
    return last_time

    
def load_geojsons(directory = 'testing', timed = True):
    conn = psycopg2.connect(psql_creds)
    cursor = conn.cursor()
    filepaths = glob.glob(f'{directory}/*.zstd')

    update_number = 0

    if timed:
        last_time = figure_out_files_to_add(cursor)     

    for path in filepaths:
        file_time = int(path.split('_')[-1].split('.')[0])
        if timed:
             if file_time <= last_time:
                  continue
             
        with open(path, 'rb') as opened:
            decoded = geobuf.decode(zstd.decompress(opened.read()))
        timestamp = datetime.datetime.fromtimestamp(file_time)
        values = []
        
        for feature in decoded['features']: 
            geom = json.dumps(feature['geometry'])
            values.append((timestamp, geom))
        
        insert_query = 'insert into twkboutagepolygons (timestamp, twkbgeom) values %s'
        psycopg2.extras.execute_values(cursor, insert_query, values, template='(%s, ST_AsTWKB(ST_GeomFromGeoJSON(%s), 6))', page_size=1000)    
        conn.commit()        
        update_number += 1

    conn.close()

    if update_number == 0:
        send_email('No new file were written to disk in 30 minutes')

def main():
    while True:
        try:
            load_geojsons(directory = '/rawfiles', timed = True)
            time.sleep(30*60)
        except Exception as e:
            traceback.print_exc()
            try:
                send_email(e)
            except:
                print('mailgun does not work')
            time.sleep(30*60)
            
main()

"""create index geom_index on twkboutagepolygons using 
gist (st_setsrid(st_geomfromtwkb(twkbgeom), 4326))"""

'''create view outagepolygon_view as select id, 
timestamp, st_setsrid(st_geomfromtwkb(twkbgeom), 4326) as geom from twkboutagepolygons;'''