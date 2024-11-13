def query(spark, table_name="taxi_trips"):
    query = f"""
    SELECT
        passenger_count,
        speed_bin,
        COUNT(*) AS total_trips,
        AVG(fare_amount) AS avg_fare_amount,
        SUM(tip_amount) AS total_tip_amount
    FROM
        {table_name}
    GROUP BY
        passenger_count,
        speed_bin
    ORDER BY
        total_trips DESC
    LIMIT 5
    """
    
    result = spark.sql(query)
    result.show()

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TripDataAnalysis").getOrCreate()
    query(spark)
    