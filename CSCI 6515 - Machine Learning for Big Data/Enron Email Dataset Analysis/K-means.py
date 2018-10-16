#This has been taken and edited as per task from http://spark.apache.org/docs/latest/mllib-clustering.html#k-means

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from pyspark import SparkContext
import sys
import os

sc =SparkContext()
for route,directories,files in os.walk('/media/deepak/data_words.csv'):
    for file in files:
        f_name=os.path.join(route,file).split('/')
        # Load and Parse the data
        data = sc.textFile(os.path.join(route,file))
        dataParsed = data.map(lambda line: array([float(x) for x in line.split(',')]))
        # Build the model (cluster the data)
        clusters = KMeans.train(dataParsed, 4, maxIterations=10,
                runs=10, initializationMode="random")
        # Evaluate clustering by computing Within Set Sum of Squared Errors
        def error(point):
            center = clusters.centers[clusters.predict(point)]
            return sqrt(sum([x**2 for x in (point - center)]))
        WSSSE = dataParsed.map(lambda point: error(point)).reduce(lambda x, y: x + y)
        print(f_name[-1])
        print('\n')
        print("Within Set Sum of Squared Error is " + str(WSSSE))
        print('\n')
        print(clusters.clusterCenters)
        # Save and load model
        clusters.save(sc, "/media/deepak/Kmean_output/"+f_name[-1])
        sameModel = KMeansModel.load(sc, "/media/deepak/Kmean_output/"+f_name[-1])