import json
import requests
from urllib import urlopen
from pyspark import SparkContext, sqlContext


DB_URL = 'jdbc:postgresql://{db_host}:{db_port}/{database}?user={db_user}'.format(
    db_host='docker.for.mac.localhost', 
    db_port=5432, 
    database='healthdb',
    db_user='andrew',
)
sc = SparkContext.getOrCreate()

db_data = spark.read.format('jdbc').options(
    url=DB_URL, dbtable='patient', driver='org.postgresql.Driver'
).load()

def parse_dataframe(json_data):
    mylist = []
    for line in json_data.splitlines():
        mylist.append(line)
    rdd = sc.parallelize(mylist)
    df = sqlContext.read.json(rdd)
    return df

def df_from_url(url):
    response = urlopen(url)
    data = str(response.read())
    return parse_dataframe(data)

url_observation = "https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Observation.ndjson"
url_patient = "https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Patient.ndjson"
url_procedure = "https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Procedure.ndjson"
url_encounter = "https://raw.githubusercontent.com/smart-on-fhir/flat-fhir-files/master/r3/Encounter.ndjson"

observation_df = df_from_url(url_observation)
patient_df = df_from_url(url_patient)
procedure_df = df_from_url(url_procedure)
encounter_df = df_from_url(url_encounter)
