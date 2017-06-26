from flask import Flask
from flask import send_file
from flask import g, request
import pandas as pd
import subprocess
import boto3
import psycopg2
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from nltk.stem import SnowballStemmer
from nltk import word_tokenize
import json

# Flask app should start in global layout
app = Flask(__name__)

with open('config.json') as config_file:
    login = json.load(config_file)
    
    database = login["database"]
    user = login["user"]
    password = login["password"]
    host = login["host"]
    port = login["port"]
    aws_access_key_id = login['aws_access_key_id']
    aws_secret_access_key = login['aws_secret_access_key']
    

def turn_list_to_str(input):
    if str(type(input)) == "<class 'list'>":
        return " ".join(input)
    else:
        return input

class StemTokenizer(object):
    def __init__(self):
        self.snbl = SnowballStemmer('english')

    def __call__(self, doc):
        return [self.snbl.stem(t) for t in word_tokenize(doc)]

stemmer = SnowballStemmer('english')
def stem_sentence(sentence):
    stemmed = []
    for word in word_tokenize(sentence):
        stemmed.append(stemmer.stem(word))
    return " ".join(stemmed)

def create_table_name(id):
    return 'table_' + id.replace('-', '_')

def get_conn():
    return psycopg2.connect(database=database, user=user, password=password,
        host=host, port=port)

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'pg_conn'):
        g.pg_conn = get_conn()
    return g.pg_conn



s3Prefix = 'https://s3.amazonaws.com/codex-images/'

running_number = 0

conn = get_conn()

data = pd.read_sql(sql="""
        select * from
        (
        select 
            metadata->>'title' as package_title,
            metadata->'description' as package_description,
            metadata->'keywords' as keywords,
            jsonb_array_elements(metadata->'resources')->'description' as resource_description, 
            jsonb_array_elements(metadata->'resources')->>'identifier' as identifier, 
            jsonb_array_elements(metadata->'resources')->>'format' as resource_format
        from package
        ) t where t.resource_format = 'CSV' 
    """, con=conn)

print('connected!')

data['info'] = data.apply(
    lambda row: row.package_title + " "
                + turn_list_to_str(row.package_description) + " "
                + turn_list_to_str(row.keywords), axis=1)
vectorizer = TfidfVectorizer(tokenizer=StemTokenizer()).fit(data['info'])
tf_idf_matrix = vectorizer.transform(data['info'])
conn.close()

print('ready!')



@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'pg_conn'):
        g.pg_conn.close()

@app.route("/search")
def query():
    # to test, use:
    # /search?query=3G%20Public%20Cellular%20Mobile%20Telephone%20Services%20-%20Average%20Success%20Rate%20Across%20All%20Cells
    
    print('Search called')
    
    global running_number
    
    query_str = request.args.get('query')
    query = [stem_sentence(query_str)]
    query_vector = vectorizer.transform(query)
    cosine_similarities = linear_kernel(query_vector, tf_idf_matrix).flatten()
    related_docs_indices = cosine_similarities.argsort()[:-6:-1]
    result = data['identifier'][related_docs_indices]
    
    sql_str = "select * from " + create_table_name(result.values.tolist()[0])

    selected_result = pd.read_sql(sql=sql_str, con=get_db())
    selected_result.head().to_html('table.html')
    
    image_name = 'table' + str(running_number) + '.png'
    subprocess.call('/usr/bin/wkhtmltoimage -f png --width 0 table.html ' + image_name, shell=True)
    
    client = boto3.client('s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    client.upload_file(image_name, 'codex-images', image_name, ExtraArgs={'ACL': 'public-read'})
    
    running_number = running_number + 1
    
    return s3Prefix + image_name



if __name__ == '__main__':
    app.run(debug=False, port=80, host='0.0.0.0')
