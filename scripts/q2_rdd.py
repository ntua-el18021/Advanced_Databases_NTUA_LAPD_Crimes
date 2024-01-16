from utilities_functions import setup_output_path, read_saved_dataframe, close_stdout
from utilities_config import *
from pyspark.sql import SparkSession
import time, os


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
    
def process_data(final_df_rdd):
    """
    Processes the data.
    """
    pair_rdd = final_df_rdd.map(lambda row: (time_of_day(int(row['TIME OCC'])//100), 1) if row['Premis Desc'] == 'STREET' else None)
    filtered_rdd = pair_rdd.filter(lambda x: x is not None)
    result_rdd = filtered_rdd.reduceByKey(lambda a, b: a + b)
    sorted_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

    return sorted_rdd.collect()



def main():
    """
    Main Function.
    """
    # Setup output path
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(DEFAULT_OUTPUT_PATH, script_name)

    # Initialize Spark session
    spark = SparkSession.builder.appName(script_name).config("spark.executor.instances", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load the combined dataset
    final_df = read_saved_dataframe(spark, SAVED_DATAFRAME_PATH, SAVED_DATAFRAME_NAME)
    final_df_rdd = final_df.rdd

    # Process data
    start_time = time.time()
    result = process_data(final_df_rdd)
    end_time = time.time()

    # Requested information!
    print("Number of Crimes occurring in (STREET) for times of day:")
    print("+---------+------+")  # Top border
    print("|TimeOfDay| Count|")  # Header
    print("+---------+------+")  # Separator
    for time_of_day, count in result:
        print("|{:<9}|{:>6}|".format(time_of_day, count))  # Data rows
    print("+---------+------+")  # Bottom border
    print(f"\nNumber of rows: {len(result)}")
    print(f"\nExecution time: {end_time - start_time} seconds")

    # Reset stdout and stop Spark session
    close_stdout()
    spark.stop()


if __name__ == "__main__":
    main()