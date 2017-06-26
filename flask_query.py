import psycopg2
import csv
import io
import sys
import pandas as pd
from urllib.request import Request
from urllib.request import urlopen
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from nltk import word_tokenize
from nltk.stem import SnowballStemmer
from flask import Flask,g,request

app = Flask(__name__)

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
    return psycopg2.connect(dbname="", user="", password="")

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'pg_conn'):
        g.pg_conn = get_conn()
    return g.pg_conn

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
data['info'] = data.apply(
    lambda row: row.package_title + " "
                + turn_list_to_str(row.package_description) + " "
                + turn_list_to_str(row.keywords), axis=1)
vectorizer = TfidfVectorizer(tokenizer=StemTokenizer()).fit(data['info'])
tf_idf_matrix = vectorizer.transform(data['info'])
conn.close()

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'pg_conn'):
        g.pg_conn.close()

@app.route("/search")
def query():
    # to test, use:
    # /search?query=3G%20Public%20Cellular%20Mobile%20Telephone%20Services%20-%20Average%20Success%20Rate%20Across%20All%20Cells
    query_str = request.args.get('query')
    query = [stem_sentence(query_str)]
    query_vector = vectorizer.transform(query)
    cosine_similarities = linear_kernel(query_vector, tf_idf_matrix).flatten()
    related_docs_indices = cosine_similarities.argsort()[:-6:-1]
    result = data['identifier'][related_docs_indices]
    sql_str = "select * from " + create_table_name(result[0])
    selected_result = pd.read_sql(sql=sql_str, con=get_db())
    return selected_result.to_html()

if __name__ == "__main__":
    app.run()