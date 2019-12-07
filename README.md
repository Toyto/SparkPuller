# SparkPuller

Getting started:
1. Copy paste sql from init.sql into your rdbms or psql shell.
2. docker build -t pyspark .
3. docker run -it --rm pyspark
4. pyspark --jars ../postgresql-42.2.8.jar
5. Copy paste script.py into pyspark shell.

Hint!
You probably would need to change the db user in code(script.py:18).
