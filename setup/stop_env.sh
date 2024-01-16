#!/bin/bash

# Stop Spark history server
echo "----------- Stopping Spark history server -----------"

$SPARK_HOME/sbin/stop-history-server.sh
echo "Spark history server stopped."

# Stop YARN services
# echo "Stopping YARN services..."
stop-yarn.sh
echo "YARN services stopped."

# Stop HDFS services
# echo "Stopping HDFS services..."
stop-dfs.sh
echo "HDFS services stopped."

echo "----------- Running Services (jps) -----------"
jps
echo "----------- All services stopped successfully -----------"
