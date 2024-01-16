from pyspark.sql.functions import col, when
from prettytable import PrettyTable
import sys
import argparse

# General Functions
def setup_output_path(output_path, script_name, with_desccriptions=False):
    """
    Sets up the path for the output file.
    """
    script_name = script_name.split(".")[0]
    if with_desccriptions:
        output_script_path = output_path+script_name+"_desc_output.txt"
    else:
        output_script_path = output_path+script_name+"_output.txt"

    sys.stdout = open(output_script_path, 'w')

    return script_name

def close_stdout():
    """
    Closes stdout.
    """
    sys.stdout.close()
    sys.stdout = sys.__stdout__

def read_saved_dataframe(spark, saved_dataframe_path, saved_dataframe_name):
    """
    Reads the saved DataFrame CSV.
    """
    full_path = saved_dataframe_path + saved_dataframe_name
    final_df = spark.read.csv(full_path, header=True, inferSchema=True)
    return final_df

# Query 3 Specific Functions
def query_3_parse_arguments():
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description='Query 3 Dataframe Construction Script')
    parser.add_argument('-d', '--descriptive', action='store_true', help='Output Descriptive Information')
    args = parser.parse_args()
   
    return args.descriptive

def query_3_print_descendants(df_descents):
    """
    Prints the descendants for Query 3.
    """
    descent_mapping = {
        'W': 'White',
        'B': 'Black',
        'H': 'Hispanic/Latin/Mexican',
        'X': 'Other'
    }
    
    df_descents_mapped = df_descents.withColumn(
        "Vict Descent",
        when(col("Vict Descent") == "W", descent_mapping["W"])
        .when(col("Vict Descent") == "B", descent_mapping["B"])
        .when(col("Vict Descent") == "H", descent_mapping["H"])
        .when(col("Vict Descent") == "X", descent_mapping["X"])
    ).filter(col("Vict Descent").isNotNull())

    df_descents_mapped.show()



# Query 2 Specific Functions
def query_2_parse_arguments(default_output_path):
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description='Query 2 Dataframe Construction Script')
    parser.add_argument('-d', '--descriptive', action='store_true', help='Output Descriptive Information')
    parser.add_argument('-p', '--path', type=str, default=default_output_path, help='Path to Output Directory')
    args = parser.parse_args()
   
    return args.descriptive
   

def query_2_print_additional_info(with_desccriptions, final_df, \
                                    non_street_crimes_df, street_crimes_df, \
                                    street_crimes_df_with_time_of_day, \
                                    time_of_day_crime_counts):
    """
    Prints additional information for Query 2.
    """
    if(with_desccriptions):
        print("/n===================================")
        print(f"Number of rows in original DB: {final_df.count()}")

        print("\n===================================")
        print(f"Number of rows (not STREET crimes): {non_street_crimes_df.count()}")

        print("\n===================================")
        print("First 10 rows of the original data (columns: TIME OCC, PREMISC DESC) :")
        street_crimes_df.show(10)
        print(f"Number of rows: {street_crimes_df.count()}")

        print("\n===================================")
        print("First 10 rows of data with added HOUR and TIMEOFDAY: ")
        street_crimes_df_with_time_of_day.show(10)
        print(f"Number of rows: {street_crimes_df_with_time_of_day.count()}")


        print("\n===================================")
        print("First 10 rows of data GROUPED: ")
        time_of_day_crime_counts.show(10)
        print(f"Number of rows: {time_of_day_crime_counts.count()}")





# Dataframe Specific Functions
def dataframe_parse_arguments(default_output_path):
    """
    Parses command line arguments.
    """
    parser = argparse.ArgumentParser(description='Dataframe Construction Script')
    parser.add_argument('-d', '--descriptive', action='store_true', help='Output Descriptive Information')
    parser.add_argument('-s', '--save', action='store_true', help='Save Final DataFrame to CSV')
    parser.add_argument('-p', '--path', type=str, default=default_output_path, help='Path to Output Directory')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Overwrite saved CSV file')
    args = parser.parse_args()
   
    return args.descriptive, args.save, args.overwrite, args.path


def dataframe_print_additional_info(with_desccriptions, rev_geo_df, income_2015_df, final_df, combined_df_original, combined_df, combined_with_zip_df, final_combined_df, df1, df2):
    # Helper function to create a table for data types
    def create_data_type_table(df, description):
        table = PrettyTable()
        table.title = f"{description} - Data Types"
        table.field_names = ["Column Name", "Type"]
        for column, dtype in df.dtypes:
            table.add_row([column, dtype])
        print(table)

    if with_desccriptions:
        print("\n---------------------- Reverse Geocoding Data (First 5 Rows): ----------------------")
        rev_geo_df.show(5)
        
        print("\n---------------------- Income 2015 Data (First 5 Rows): ----------------------")
        income_2015_df.show(5)

        print("\n---------------------- Final Combined DataFrame (First 5 Rows): ----------------------")
        final_df.show(5)

        print("\n---------------------- Original Dataset Information: ----------------------")
        print(f"Number of Rows: {combined_df_original.count()}")
        print(f"Number of Columns: {len(combined_df_original.columns)}")
        create_data_type_table(combined_df_original, "Original Dataset")

        print("\n---------------------- Reverse Geocoding Data Information: ----------------------")
        print(f"Number of Rows: {rev_geo_df.count()}")
        create_data_type_table(rev_geo_df, "Reverse Geocoding Data")

        print("\n---------------------- Income 2015 Data Information: ----------------------")
        print(f"Number of Rows: {income_2015_df.count()}")
        create_data_type_table(income_2015_df, "Income 2015 Data")

        print("\n---------------------- Final Combined DataFrame Information: ----------------------")
        print(f"Number of Rows: {final_df.count()}")
        print(f"Number of Columns: {len(final_df.columns)}")
        create_data_type_table(final_df, "Final Combined DataFrame")

        print("\n---------------------- Additional Information: ----------------------")
        print(f"Unique LAT/LONG pairs in Original Dataset: {combined_df.select('LAT', 'LON').distinct().count()}")
        print(f"Paired ZIP codes in Final DataFrame: {final_df.filter(col('ZIPcode').isNotNull()).count()}")
        print(f"Paired Community names in Final DataFrame: {final_df.filter(col('Community').isNotNull()).count()}")
        print(f"NULL ZIP codes in Final DataFrame: {final_df.filter(col('ZIPcode').isNull()).count()}")
        print(f"NULL Community names in Final DataFrame: {final_df.filter(col('Community').isNull()).count()}")
        print("\t\t\t----------------------------------\t\t\t")
        print(f"Number of rows in Dataframe 1: {df1.count()}")
        print(f"Number of rows in Dataframe 2: {df2.count()}")
        print(f"Number of rows in Original Dataset (after union): {combined_df_original.count()}")
        print(f"Number of rows in Dataframe WITH ZIP codes: {combined_with_zip_df.count()}")
        print(f"Number of rows in WITH ZIP Dataframe, joined with Income 2015: {final_combined_df.count()}")
        print(f"Number of rows in Final Combined DataFrame: {final_df.count()}")
        print("\n---------------------- Used for Query 2 ----------------------")
        print("===> Rev Geocoding Dataframe:")
        print(f"Number of rows: {rev_geo_df.count()}")
        print(f"Number of unique LAT/LONG pairs: {rev_geo_df.select('LAT', 'LON').distinct().count()}")
        print(f"Number of unique ZIP codes: {rev_geo_df.select('ZIPcode').distinct().count()}")

        print("\n===> Income 2015 Dataframe:")
        print(f"Number of rows: {income_2015_df.count()}")
        print(f"Number of unique ZIP codes: {income_2015_df.select('Zip Code').distinct().count()}")
        print(f"Number of unique Community names: {income_2015_df.select('Community').distinct().count()}")
        print(f"Number of unique Median Incomes: {income_2015_df.select('Estimated Median Income').distinct().count()}")

        print("\n===> Final Combined Dataframe:")
        print(f"Number of rows: {final_df.count()}")
        print(f"\nNumber of unique LAT/LONG pairs: {final_df.select('LAT', 'LON').distinct().count()}")
        print(f"Number of NULL LAT/LONG pairs: {final_df.filter(col('LAT').isNull() | col('LON').isNull()).count()}")
        print(f"Number of non-NULL LAT/LONG pairs: {final_df.filter(col('LAT').isNotNull() & col('LON').isNotNull()).count()}")
        print(f"Number of total NULL and non-NULL LAT/LONG pairs: {final_df.filter(col('LAT').isNull() | col('LON').isNull()).count() + final_df.filter(col('LAT').isNotNull() & col('LON').isNotNull()).count()}")

        print(f"\nNumber of unique ZIP codes: {final_df.select('ZIPcode').distinct().count()}")
        print(f"Number of NULL ZIP codes: {final_df.filter(col('ZIPcode').isNull()).count()}")
        print(f"Number of non-NULL ZIP codes: {final_df.filter(col('ZIPcode').isNotNull()).count()}")
        print(f"Number of total NULL and non-NULL ZIP codes: {final_df.filter(col('ZIPcode').isNull()).count() + final_df.filter(col('ZIPcode').isNotNull()).count()}")

        print(f"\nNumber of unique Community names: {final_df.select('Community').distinct().count()}")
        print(f"\nNumber of unique Median Incomes: {final_df.select('Estimated Median Income').distinct().count()}")
        print(f"Number of NULL Median Incomes: {final_df.filter(col('Estimated Median Income').isNull()).count()}")
        print(f"Number of non-NULL Median Incomes: {final_df.filter(col('Estimated Median Income').isNotNull()).count()}")
        print(f"Number of total NULL and non-NULL Median Incomes: {final_df.filter(col('Estimated Median Income').isNull()).count() + final_df.filter(col('Estimated Median Income').isNotNull()).count()}")


def dataframe_print_requested_information(final_df):
    print("\n---------------------- Requested Information ----------------------")
    print(f"Number of Rows in Final DataFrame: {final_df.count()}")
    table = PrettyTable()
    table.field_names = ["Column Name", "Type"]
    for column, dtype in final_df.dtypes:
        table.add_row([column, dtype])
    print("\nColumn Types for Final Combined DataFrame:")
    print(table)


def save_dataframe_to_csv(overwrite_csv, save_to_csv, saved_dataframe_name, final_df):
    if overwrite_csv:
        final_df.write.mode("overwrite").csv(saved_dataframe_name, header=True)
        print("\nFinal DataFrame successfully saved to CSV")
    elif save_to_csv:
        final_df.write.csv(saved_dataframe_name, header=True)
        print("\nFinal DataFrame successfully saved to CSV")