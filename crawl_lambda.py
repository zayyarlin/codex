import csv
import io
import psycopg2
from psycopg2.extras import Json
from urllib.request import Request
from urllib.request import urlopen
from ckanapi import RemoteCKAN

data_gov_ckan = RemoteCKAN('https://data.gov.sg/', user_agent='')
conn = psycopg2.connect(database='', user='', password="",
        host='', port='5432')
cur = conn.cursor()

# Access the resource and return a CSV reader
def access_resource(resource_url):
    hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}
    req = Request(resource_url, headers=hdr)
    response = urlopen(req).read().decode('utf-8')
    return csv.reader(io.StringIO(response))

def read_metadata(package_id):
    print('reading ' + package_id)
    return data_gov_ckan.action.package_metadata_show(id=package_id)

def save_package_metadata(package_metadata):
    cur.execute("insert into package (id, metadata) values (%s, %s)", (package_metadata['name'], Json(package_metadata)))

# main logic
packages = data_gov_ckan.action.package_list()
for package in packages:
    save_package_metadata(read_metadata(package))

conn.commit()
cur.close()
conn.close()
print('---------------')
print('done!')

