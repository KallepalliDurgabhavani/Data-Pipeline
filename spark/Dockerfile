FROM jupyter/pyspark-notebook:latest

USER root

# Only PostgreSQL JDBC (small download)
RUN wget -P /usr/local/spark/jars https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app
COPY app/ .

EXPOSE 4040
CMD ["/usr/local/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--master", "local[*]", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoint", \
     "streaming_app.py"]
