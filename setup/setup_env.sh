#!/bin/bash

# Start HDFS services
echo "----------- Starting Environment -----------"
start-dfs.sh
echo "HDFS services started."

# Start YARN services
# echo "Starting YARN services..."
start-yarn.sh
echo "YARN services started."

# Start Spark history server
# echo "Starting Spark history server..."
$SPARK_HOME/sbin/start-history-server.sh
echo "Spark history server started."


echo "----------- Running Services (jps) -----------"
jps
echo ""----------- All services started successfully "-----------"
