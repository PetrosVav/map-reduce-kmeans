from pyspark import SparkContext, SparkConf
from math import radians, sin, cos, sqrt, asin

def haversine(X, lat2, lon2):
        lat1, lon1 = X

        R = 6372.8  # Earth radius in kilometers

        dLat = radians(lat2 - lat1)
        dLon = radians(lon2 - lon1)
        lat1 = radians(lat1)
        lat2 = radians(lat2)

        a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
        c = 2 * asin(sqrt(a))

        return R * c

def get_labels(point, centroids):
        index = 0
        min_distance = haversine(point, centroids[0][0], centroids[0][1])
        for i in range(len(centroids)-1):
                distance = haversine(point, centroids[i+1][0], centroids[i+1][1])
                if (distance < min_distance):
                        min_distance = distance
                        index = i+1
        return index

conf = SparkConf().setAppName('kMeans')
sc = SparkContext(conf=conf)

data = sc.textFile("hdfs://master:9000/yellow_tripdata_1m.csv", 50)
data = data.map(lambda line: line.split(','))
data = data.map(lambda line: (float(line[3]), float(line[4])))
data = data.filter(lambda (x,y): (x!=0 and y!=0))
data = data.cache()

k = 5
ITERATIONS = 3
centroids = data.take(k)

for i in range(ITERATIONS):
        # For each point in the dataset, chose the closest centroid.
        # Make that centroid's index the point's label.
        points_and_labels = data.map(lambda point: (get_labels(point, centroids), (point, 1)))

        # Each new centroid is the mean of the points that have the
        # same centroid's label.
        points_and_labels = points_and_labels.reduceByKey(lambda a, b: ((a[0][0]+b[0][0], a[0][1]+b[0][1]), a[1]+b[1]) )
        centroids = points_and_labels.mapValues(lambda x: (x[0][0]/x[1], x[0][1]/x[1])).collectAsMap()

print"\nCentroid Coordinates"
output = [('Centroid', 'Coordinates')]
for i in range(k):
        output.append([(i+1,centroids[i])])
        print i+1, centroids[i]
print ""

centroids = sc.parallelize(centroids)
centroids.saveAsTextFile("hdfs://master:9000/kmeans.res")

