import json
from urllib import urlopen
from pyspark import SparkContext
from pyspark.sql.functions import (
    col,
    to_date,
    to_timestamp,
    length,
    coalesce,
    date_format,
)


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

patients_by_races = patient_df.select(
    col('id').alias('source_id'),
    to_date('birthDate').alias('birth_date'),
    col('gender'),
    col('address')[0]['country'].alias('country'),
    col('extension')['valueCodeableConcept'][
        'coding'][0]['code'][0].alias('race_code'),
    col('extension')['valueCodeableConcept']['coding'][
        0]['system'][0].alias('race_code_system'),
).filter(
    col('extension')['url'][
        0] == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-race',
)

patients_by_ethnicity = patient_df.select(
    col('id').alias('source_id_dummy'),
    col('extension')['valueCodeableConcept']['coding'][
        0]['code'][0].alias('ethnicity_code'),
    col('extension')['valueCodeableConcept']['coding'][
        0]['system'][0].alias('ethnicity_code_system'),
).filter(
    col('extension')['url'][
        0] == 'http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity',
)

patients = patients_by_races.join(
    patients_by_ethnicity,
    patients_by_races.source_id == patients_by_ethnicity.source_id_dummy,
    how='left_outer',
).drop(col('source_id_dummy'))

encounters = encounter_df.select(
    col('id').alias('source_id'),
    col('subject')['reference'].substr(9, 36).alias('patient_id'),  # hack to get just a patient_id
    to_timestamp(col('period')['start']).alias('start_date'),
    to_timestamp(col('period')['end']).alias('end_date'),
    col('type')[0]['coding'][0]['code'].alias('type_code'),
    col('type')[0]['coding'][0]['system'].alias('type_code_system'),
).filter(
    length(col('patient_id')) == 36 # filter out incorrect patient ids
)

procedures = procedure_df.select(
    col('id').alias('source_id'), 
    col('subject')['reference'].substr(9, 36).alias('patient_id'), 
    col('context')['reference'].substr(11, 36).alias('encounter_id'), 
    coalesce(
        to_date('performedDateTime'), 
        to_date(col('performedPeriod')['start'])
    ).alias('procedure_date'), 
    col('code')['coding'][0]['code'].alias('type_code'), 
    col('code')['coding'][0]['system'].alias('type_code_system'),
).filter(
    length(col('patient_id')) == 36
).filter(
    length(col('encounter_id')) == 36
)

observations = observation_df.select(
    col('id').alias('source_id'), 
    col('subject')['reference'].substr(9, 36).alias('patient_id'), 
    col('context')['reference'].substr(11, 36).alias('encounter_id'),
    to_date('effectiveDateTime').alias('observation_date'),
    coalesce(
        col('code')['coding'][0]['code'], 
        col('component')['code']['coding'][0]['code'][0],
    ).alias('type_code'),
    coalesce(
        col('code')['coding'][0]['system'], 
        col('component')['code']['coding'][0]['system'][0],
    ).alias('type_code_system'),
    coalesce(
        col('valueQuantity')['value'], 
        col('component')['valueQuantity']['value'][0],
    ).alias('value'),
    coalesce(
        col('valueQuantity')['unit'], 
        col('component')['valueQuantity']['unit'][0],
    ).alias('unit_code'),
    coalesce(
        col('valueQuantity')['system'], 
        col('component')['valueQuantity']['system'][0],
    ).alias('unit_code_system'),
).filter(
    length(col('patient_id')) == 36
).filter(
    length(col('encounter_id')) == 36
).filter(
    col('value').isNotNull()
)

patients.write.format('jdbc').options(
    url=DB_URL,
    dbtable='patient',
    driver='org.postgresql.Driver',
    stringtype='unspecified',
).mode('append').save()

encounters.write.format('jdbc').options(
    url=DB_URL,
    dbtable='encounter',
    driver='org.postgresql.Driver',
    stringtype='unspecified',
).mode('append').save()

procedures.write.format('jdbc').options(
    url=DB_URL,
    dbtable='procedure',
    driver='org.postgresql.Driver',
    stringtype='unspecified',
).mode('append').save()

observations.write.format('jdbc').options(
    url=DB_URL,
    dbtable='observation',
    driver='org.postgresql.Driver',
    stringtype='unspecified',
).mode('append').save()

print 'Patients: {}'.format(patients.count())
print 'Encounters: {}'.format(encounters.count())
print 'Procedures: {}'.format(procedures.count())
print 'Observations: {}'.format(observations.count())
print 'Patients by gender: {}'.format(patients.groupBy(col('gender')).count())

print 'Top 10 procedures'
procedures.groupBy(col('type_code')).count().orderBy(col('count').desc()).limit(10).show()

print 'The most popular days for encounter'
encounters.select(
    date_format(
        col('start_date'), 'E'
    ).alias(
        'week_day'
    )
).groupBy(
    'week_day'
).count(
).orderBy(
    col('count').desc()
).limit(3).show()

print 'The least popular days for encounters'
encounters.select(
    date_format(
        col('start_date'), 'E'
    ).alias(
        'week_day'
    )
).groupBy(
    'week_day'
).count(
).orderBy(
    col('count')
).limit(4).show()
