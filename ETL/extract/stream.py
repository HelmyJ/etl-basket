import os
import requests
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class DatabaseConnection:
    def __init__(self):
        self.host = os.getenv('PG_HOST')
        self.port = os.getenv('PG_PORT')
        self.dbname = os.getenv('PG_DBNAME')
        self.user = os.getenv('PG_USER')
        self.password = os.getenv('PG_PASSWORD')
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.user,
                password=self.password
            )
            print("Connection to the database established successfully.")
        except psycopg2.Error as e:
            print(f"Error: Could not connect to the database.\n{e}")

    def execute_query(self, query, data=None):
        try:
            cursor = self.connection.cursor()
            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
            print("Query executed successfully.")
        except psycopg2.Error as e:
            print(f"Error: Could not execute query.\n{e}")

    def close(self):
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

def write_to_postgres(df, epoch_id):
    db_connection = DatabaseConnection()
    db_connection.connect()
    for row in df.collect():
        db_connection.execute_query(
            "INSERT INTO basketball_stats (player_name, team_name, points, assists, rebounds, steals, blocks) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (row.player_name, row.team_name, row.points, row.assists, row.rebounds, row.steals, row.blocks)
        )
    db_connection.close()

if __name__ == "__main__":
    # Define the schema for the incoming data
    schema = StructType([
        StructField("player_name", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("points", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("rebounds", IntegerType(), True),
        StructField("steals", IntegerType(), True),
        StructField("blocks", IntegerType(), True)
    ])

    # Initialize Spark session
    spark = SparkSession.builder.appName("NBADataStream").getOrCreate()

    # Read data from Kafka
    raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "nba_data").load()

    # Parse the JSON data
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Write the data to PostgreSQL
    query = parsed_df.writeStream.foreachBatch(write_to_postgres).start()

    query.awaitTermination()