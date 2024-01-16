echo "Running all scripts!"
echo -e "-------------------------\n"
echo "Running utilites_create_dataframe.py"
echo "-------------------------"
# spark-submit utilites_create_dataframe.py -d
spark-submit utilities_create_dataframe.py -o -s

echo -e "\n\n\n\n-------------------------\n"
echo "Running Query 1 scripts"
echo "-------------------------"
spark-submit q1_dataframe.py
spark-submit q1_sql.py

echo -e "\n\n\n\n-------------------------\n"
echo "Running Query 2 scripts"
echo "-------------------------"
spark-submit q2_dataframe.py
spark-submit q2_rdd.py

echo -e "\n\n\n\n-------------------------\n"
echo "Running Query 3 script"
echo "-------------------------"
spark-submit q3_dataframe.py

echo -e "\n\n\n\n-------------------------\n"
echo "Running Query 4 script"
echo "-------------------------"
spark-submit q4_dataframe.py

echo -e "\n\n\n\n-------------------------\n"
echo "Running analysis_compare.py"
echo "-------------------------"
spark-submit analysis_compare_joins.py