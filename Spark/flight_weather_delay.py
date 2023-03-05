import argparse

from pyspark.sql import SparkSession

def calculate_avg_delay(data_source, output_uri):
    
    with SparkSession.builder.appName("Calculate Average Delay").getOrCreate() as spark:
        # Load the flight delay CSV data
        if data_source is not None:
            flight_df = spark.read.option("header", "true").csv(data_source)

        # Create an in-memory DataFrame to query
        flight_df.createOrReplaceTempView("delay_flights")
        
        avg_weather_delay = spark.sql("""SELECT Year, avg((WeatherDelay /ArrDelay)*100) 
          FROM delay_flights 
          GROUP BY Year
          ORDER BY Year ASC""")

        # Write the results to the specified output URI
        avg_weather_delay.write.option("header", "true").mode("overwrite").csv(output_uri)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source', help="The URI for you CSV flight data, like an S3 bucket location.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    args = parser.parse_args()

    calculate_avg_delay(args.data_source, args.output_uri)