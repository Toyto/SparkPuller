DB_URL = 'jdbc:postgresql://{db_host}:{db_port}/{database}?user={db_user}'.format(
    db_host='docker.for.mac.localhost', 
    db_port=5432, 
    database='healthdb',
    db_user='andrew',
)
db_data = spark.read.format('jdbc').options(
    url=DB_URL, dbtable='patient', driver='org.postgresql.Driver'
).load()