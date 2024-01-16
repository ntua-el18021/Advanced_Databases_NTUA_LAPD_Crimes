from utilities_functions import query_2_parse_arguments, query_2_print_additional_info, setup_output_path, close_stdout, read_saved_dataframe
from utilities_config import *
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
import os, time


def time_of_day(hour):
    """ 
    Returns the time of day given the hour.
    """
    if 5 <= hour < 12:
        return 'Morning'
    elif 12 <= hour < 17:
        return 'Afternoon'
    elif 17 <= hour < 21:
        return 'Evening'
    else:
        return 'Night'


def process_data(final_df):
    """
    Processes the data.
    """
    non_street_crimes_df = final_df.filter(F.col("Premis Desc") != "STREET")
    street_crimes_df = final_df.filter(F.col("Premis Desc") == "STREET")

    time_of_day_udf = F.udf(time_of_day)

    street_crimes_df_with_time_of_day = street_crimes_df.withColumn("Hour", (F.col("TIME OCC") / 100).cast('integer')) \
                                                        .withColumn("TimeOfDay", time_of_day_udf("Hour"))

    time_of_day_crime_counts = street_crimes_df_with_time_of_day.groupBy("TimeOfDay").count()

    # Sort the result in descending order of count
    sorted_time_of_day_crime_counts = time_of_day_crime_counts.orderBy(F.desc("count"))

    return non_street_crimes_df, street_crimes_df, street_crimes_df_with_time_of_day, time_of_day_crime_counts, sorted_time_of_day_crime_counts



def main():
    """
    Main Function.
    """
    # Parse arguments and setup output path
    with_desccriptions = query_2_parse_arguments(DEFAULT_OUTPUT_PATH)
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(DEFAULT_OUTPUT_PATH, script_name, with_desccriptions)

    # Initialize Spark session
    spark = SparkSession.builder.appName(script_name).config("spark.executor.instances", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load the combined dataset
    final_df = read_saved_dataframe(spark, SAVED_DATAFRAME_PATH, SAVED_DATAFRAME_NAME)

    # Process data
    start_time = time.time()
    non_street_crimes_df, street_crimes_df, street_crimes_df_with_time_of_day, time_of_day_crime_counts, sorted_time_of_day_crime_counts = process_data(final_df)
    end_time = time.time()

    # Descriptive output
    query_2_print_additional_info(with_desccriptions, final_df, non_street_crimes_df, street_crimes_df, street_crimes_df_with_time_of_day, time_of_day_crime_counts)
    
    # Requested information!
    print("Number of Crimes occuring in (STREET) for times of day:")
    sorted_time_of_day_crime_counts.show()
    print(f"Number of rows: {sorted_time_of_day_crime_counts.count()}")
    print(f"\nExecution time: {end_time - start_time} seconds")

    # Reset stdout and stop Spark session
    close_stdout()
    spark.stop()


if __name__ == "__main__":
    main()