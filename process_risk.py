import json
import requests
import re
import sys
from itertools import product

from tqdm import tqdm  # cool progress bars
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import functions as f
from pyspark.sql import Window

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


spark = SparkSession \
        .builder \
        .appName("HEM risk") \
        .master("spark://hem1:7077")\
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


#create safer requests protocol
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


def get_hdfs_files():
    """Fetches all the files from HDFS"""
    URI           = sc._gateway.jvm.java.net.URI
    Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


    fs = FileSystem.get(URI("hdfs://hem1:9000"), Configuration())
    status = fs.listStatus(Path('/risk'))
    files = []
    for fileStatus in status:
        files.append(str(fileStatus.getPath()))

    return files


def read_csv_file(csv_file):
    #get state code
    if 'part' not in csv_file:
        statecode = csv_file[-6:][:2]
    else:
        statecode = csv_file[-12:][:2]

    #load in csv
    df = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(csv_file)

    #cast all types, every risk column is a float and population is an integer
    df = df.withColumn("population", df["population"].cast(IntegerType()))
    for col in df.columns:
        if col not in set(['state', 'region', 'county', 'tract', 'pollutant', 'population', 'fips']):
            df = df.withColumn(col, df[col].cast(FloatType()))

    return df


def clean_up_df(df):
    # we only want to select rows where the county has the highest population on record
    w = Window.partitionBy('county')
    df = df.withColumn('max_population', f.max('population').over(w))\
        .where(f.col('population') == f.col('max_population'))\
        .drop('max_population')

    return df


def publish_to_es(df):
    #get counties
    counties = df.select('county').distinct().rdd.map(lambda r: r[0]).collect()

    for county in tqdm(counties):
        if " " in county:
            county = re.sub(" ", "_", county)

        single = df.filter(df.county.like('%' + county + '%'))
        pollutants_json_list = single.toJSON().collect()
        bulk_insert_commands = []
        for item in pollutants_json_list:
            bulk_insert_commands.append('{"index": {"_index": "pollutants_by_county", "_type": "county"}}')
            bulk_insert_commands.append(item)

        r = requests.post('http://169.45.85.246:9200/pollutants_by_county/_bulk', "\n".join(bulk_insert_commands))
        r.raise_for_status()


def main():
    files = get_hdfs_files()
    for csv_file in tqdm(files):  # tqdm gives progress bars of iterators
        df = read_csv_file(csv_file)
        df = clean_up_df(df)
        publish_to_es(df)


if __name__=='__main__':
    main()
