import os
from pyspark.sql import SparkSession, functions as F

def transform(spark, file_path="/yellow_tripdata_2024-01.csv", table_name="taxi_trips"):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    df_with_duration = df.withColumn(
        'trip_duration',
        (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60
    )
    
    df_with_speed = df_with_duration.withColumn(
        'average_speed',
        F.when(F.col('trip_duration') > 0,
               F.col('trip_distance') / (F.col('trip_duration') / 60)
        ).otherwise(None)
    )
    
    df_filtered = df_with_speed.filter(
        (F.col('trip_distance') > 0) &
        (F.col('trip_duration') > 0) &
        (F.col('average_speed') >= 1) &
        (F.col('average_speed') <= 100)
    )
    
    df_binned = df_filtered.withColumn(
        'speed_bin',
        (F.floor(F.col('average_speed') / 10) * 10).cast('int')
    )
    
    # save it as a table using data sink
    df_binned.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"Data successfully saved as table '{table_name}'")

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TripDataAnalysis").getOrCreate()
    transform(spark)
