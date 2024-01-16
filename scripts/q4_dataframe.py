from utilities_functions import setup_output_path, read_saved_dataframe, close_stdout
from utilities_config import *


from pyspark.sql.session import SparkSession
import os
from pyspark.sql.functions  import col, year, count, avg, round
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.types import  DoubleType
from pyspark.sql import functions as F


def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two coordinates.
    """
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371  
    return c * r



def load_and_make_dataframe (spark):
    """
    Load the data and make a dataframe.
    """
    final_df = read_saved_dataframe(spark, SAVED_DATAFRAME_PATH, SAVED_DATAFRAME_NAME)

    final_df = final_df.withColumn("Year", year(col("DATE OCC")))
    final_df = final_df.filter(col("Year").isNotNull())
    final_df = final_df.filter((col("LAT")!=0) & (col("LON")!=0))
    final_df = final_df.filter(col("Weapon Used Cd").rlike("^1"))


    lapd_stations_df = spark.read.csv(DATASET_PATH+"LAPD_Police_Stations.csv", header=True, inferSchema=True)
    combined_df = final_df.join(lapd_stations_df, final_df["AREA"] == lapd_stations_df["PREC"])
    
    combined_df = combined_df.select(
        col("DR_NO"),
        col("DATE OCC"),
        col("Year"),
        col("Weapon Used Cd"),
        col("DIVISION").alias("Station_Division"),
        col("LAT").alias("Crime_Latitude"),
        col("LON").alias("Crime_Longitude"),
        col("Y").alias("Station_Latitude"),
        col("X").alias("Station_Longitude")
    )

    calculate_distance_udf = F.udf(calculate_distance, DoubleType())


    combined_df = combined_df.withColumn("Crime_Station_Distance", calculate_distance_udf(
        col("Crime_Latitude"), col("Crime_Longitude"),
        col("Station_Latitude"), col("Station_Longitude")))
    
    return combined_df


def process_data_by_year(combined_df):
    """
    Process the data.
    """
    yearly_data = combined_df.groupBy((col("Year")))
    aggregated_data = yearly_data.agg(count("DR_NO").alias("#"),
                                round(avg("Crime_Station_Distance"), 3).alias("average_distance (km)"))
    processed_by_year = aggregated_data.select("year", "average_distance (km)", "#")
    processed_by_year = processed_by_year.sort("year")

    return processed_by_year


def process_data_by_division(combined_df):
    """
    Process the data.
    """
    division_data = combined_df.groupBy("Station_Division")
    aggregated_data = division_data.agg(count("DR_NO").alias("#"),
                                         round(avg("Crime_Station_Distance"), 3).alias("average_distance (km)"))
    processed_by_division = aggregated_data.select("Station_Division", "average_distance (km)", "#")

    processed_by_division = processed_by_division.sort("Station_Division")
    processed_by_division = processed_by_division.sort(col("#").desc())

    return processed_by_division

    

def main ():
    """
    Main function.
    """
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(DEFAULT_OUTPUT_PATH, script_name)

    spark = SparkSession.builder.appName(script_name).config("spark.executor.instances", 4).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    combined_df = load_and_make_dataframe(spark)
    processed_by_year = process_data_by_year(combined_df)
    processed_by_year.show()

    processed_by_division = process_data_by_division(combined_df)
    processed_by_division.show(30)

    
    spark.stop()
    close_stdout()



if __name__ == "__main__":
    main()