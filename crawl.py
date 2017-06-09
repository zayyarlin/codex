import csv
import io
import boto3
from urllib.request import Request
from urllib.request import urlopen
from ckanapi import RemoteCKAN

data_gov_ckan = RemoteCKAN('https://data.gov.sg/', user_agent='')
packages = data_gov_ckan.action.package_search(q='cumulative')

len(packages)

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

def print_csv(reader):
    for row in reader:
        print(row)

for result in packages['results']:
    print("=======================")
    for resource in result['resources']:
        reader = access_resource(resource['url'])
        print("---------------------")
        print_csv(reader)

ddb2 = boto3.resource('dynamodb', endpoint_url='http://localhost:8000', region_name='us-east-1')
print(list(ddb2.tables.all()))
