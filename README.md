# Project in Advanced Databases Course (NTUA)
The current project makes use of various Databases, created by the Los Angeles Police Department (LAPD), regarding crimes that were commited from 2010, to present.
We combine said databased to create a Dataframe that is later used to perform queries, so as to extract significant information from the above data.

### Contributors
* Kazdagli Ariadne (03118838)
* Paranomos Yiannos (03118021)

### Installation Guide
Our project was implemented using 2 Virtual Machines, but can also be done by accessing online resources. The main guide for project was provided to us at the start of our assignment:
* [Google Colab Instructions](https://colab.research.google.com/drive/1eE5FXf78Vz0KmBK5W8d4EUvEFATrVLmr) 

We suggest, connecting to the Virtual Machines, initially by ssh but to then setup the necessary RSA keys to conenct effortlessly. Using the VSCode Remote Repository feature is also suggested. <br>
After the above installation steps are performed, you can stop and setup the needed environment, simply by using the scripts in the ```setup``` directory. 
* Use ```bash setup_env.sh``` to startup the environment
* Use ```stop_env.sh``` to stop the environment safely

During our testing we found that sometimes the above tasks can me memory intensive and that the cache would sometimes come dangerously close to maximum memory capabilities. For debugging purposes, try seeing your system's memory utilization: ```df -h```, and if necessary stopping and restarting the environment.

### Queries Execution
All requested scripts can be found in the ```scripts``` directory. There are 2 utility-only scripts: ```utilities_conf.py``` and ```utilities_functions.py```, with the first containing necessary paths and fixed variables and the latter, containing functions relative to input/output that were placed i na seperate file for easy of readability of the main scripts.<br>
The rest of the Queries and Scripts, can be executed by: ```spark-submit [script_name]``` , which runs the script with default parameters or all together with: ```bash run_scripts.sh``` <br>
We bring special attention to the creation of the dataframe, as that will be used by latter scripts! The script gives several options, like adding more descriptions to the output file, but most importantly, you can specify that the final dataframe is saved in your hdfs! To do so run: ```spark-submit utilities_create_dataframe.py -d```

### Resource Management
You can monitor your apps' execution and overall health of HDFS, YARN and SPARK, through the following webpages, but make sure to adjust with you master VM's public ip!
- HDFS: http://192.168.2.14:9870
- YARN: http://192.168.2.14:8088/cluster
- Spark History: http://192.168.2.14:18080