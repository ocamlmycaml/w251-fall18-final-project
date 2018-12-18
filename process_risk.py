import json
import requests
import re
import sys
from itertools import product

from tqdm import tqdm  # cool progress bars
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, FloatType
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


spark = SparkSession \
        .builder \
        .appName("HEM risk") \
        .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")


URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


fs = FileSystem.get(URI("hdfs://hem1:9000"), Configuration())


status = fs.listStatus(Path('/risk'))
files = []


for fileStatus in status:
    files.append(str(fileStatus.getPath()))


#create safer requests protocol
session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)


for csv_file in tqdm(files):  # tqdm gives progress bars of iterators

    #get state code
    if 'part' not in csv_file:
        statecode = csv_file[-6:][:2]
    else:
        statecode = csv_file[-12:][:2]

    #load in csv
    df = spark.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load(csv_file)

    #cast population as integer
    df = df.withColumn("population", df["population"].cast(IntegerType()))
    for col in df.columns:
        if col not in set(['state', 'region', 'county', 'tract', 'pollutant', 'population', 'fips']):
            df = df.withColumn(col, df[col].cast(FloatType()))

    #get counties
    counties = df.select('county').distinct().rdd.map(lambda r: r[0]).collect()

    for county in tqdm(counties):
        if " " in county:
            county = re.sub(" ", "_", county)

        single = df.filter(df.county.like('%' + county + '%'))

        # get max population so we can select the rows with this population
        top_pop = single.select("population").rdd.max()[0]

        pollutants_by_county = single.filter(single.population == top_pop)

        pollutants_json_list = pollutants_by_county.toJSON().collect()
        bulk_insert_commands = []
        for item in pollutants_json_list:
            bulk_insert_commands.append('{"index": {"_index": "counties", "_type": "county"}}')
            bulk_insert_commands.append(item)

        r = requests.post('http://169.45.85.246:9200/counties/_bulk', "\n".join(bulk_insert_commands))
        r.raise_for_status()

