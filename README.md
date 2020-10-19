# Clustering of Taxi Trip Data using Spark
## Dataset
The dataset is the 2015 Yellow Taxi Trip Data. It includes trip records from all trips completed in yellow taxis from in NYC from January to June in 2015. Due to limited resources we used a subset o 2 GB of the dataset. This subset is availlable [here](http://www.cslab.ntua.gr/courses/atds/yellow_trip_data.zip). The .zip file contains two .csv files. The first contains all the necessary information about a route and the second contains information about the taxi vendors.
## Algorithm
We implemented the K-means with k=5, that clusters the pickup locations in five regions, in order to find the coordinates of the top 5 pickup locations.
## Requirements
Spark and HDFS must be already installed in our system.
## Usage
* Upload data to the Hadoop file system
```
hadoop fs -put ./yellow_tripdata_1m.csv hdfs://master:9000/yellow_tripdata_1m.csv
```
* Install the necessary requirements
```
pip install requirements.txt
```
* Submit the job in a Spark environment
```
spark-submit kmeans.py
```
* Get the results from the HDFS and print them
```
hadoop fs -getmerge hdfs://master:9000/kmeans.res ./kmeans.res
cat kmeans.res 
```
* Convert them in kml form
```
python kml.py
```
## Results
![alt text](https://github.com/PetrosVav/map-reduce-kmeans/blob/main/results.PNG)
![alt text](https://github.com/PetrosVav/map-reduce-kmeans/blob/main/kml.png)
