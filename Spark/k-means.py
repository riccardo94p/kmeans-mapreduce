import getopt
import sys
import time
import numpy as np
from pyspark import SparkContext

import subprocess

#filename = 'output.txt'

#Default parameters
points = 10
dimension = 2
maxIterations = 10
threshold = 0.5
k = 4
seed = 1

def assignToCentroid(pointString, centroids):
	#Convert pointString to float np.array
	point = np.fromstring(pointString, count=dimension, sep=' ')
	
	#Get the index of the centroid the point belongs to
	index = np.linalg.norm(centroids - point, axis=1).argmin()
	
	#Append 1 at the end to be used as counter (of points) in reduce
	return index, np.append(point, [1])

if __name__ == "__main__":#TODO: testare se il parsing degli argomenti funziona (soprattutto il float)
	options, remainder = getopt.getopt(sys.argv[1:], 'o:p:d:k:i:t:s:', ['output=', 'num_points=', 'dimension=', 'num_k=', 'iterations=', 'threshold=', 'seed='])
	print('OPTIONS :', options)
	for opt, arg in options:
		if opt in ('-o', '--output'):
			filename = arg
		elif opt in ('-p', '--num_points'):
			points = int(arg)
		elif opt in ('-d', '--dimension'):
			dimension = int(arg)
		elif opt in ('-k', '--num_k'):
			k = int(arg)
		elif opt in ('-i', '--iterations'):
			maxIterations = int(arg)
		elif opt in ('-t', '--threshold'):
			threshold = float(arg)
		elif opt in ('-s', '--seed'):
			seed = int(arg)
	
	#TODO: controllare che k <= p

	master = "local"#TODO: passarlo come input
	sc = SparkContext(master, "k-Means")
	
	#Elimino i risultati dell'esecuzione precedente
	subprocess.call(["hadoop", "fs", "-rm", "-r", "spark-test/test_output"])
	
	#Load points from file in HDFS
	pointStrings = sc.textFile("spark-test/points_10x2.txt_test")
	#TODO: prendere solo le prime "points" righe
	
	#Select k random start points
	sample = pointStrings.takeSample(False, k, seed)
	centroids = np.array([np.fromstring(x, count=dimension, sep=' ') for x in sample])
	br_centroids = sc.broadcast(centroids)
	
	iteration = 1
	delta = float("inf")
	
	while True:
		#Perform map-reduce
		centroidPointPairs = pointStrings.map(lambda x: assignToCentroid(x, br_centroids.value)).cache()#TODO: Vedere se cache qui serve
		newCentroidsTmp = centroidPointPairs.reduceByKey(lambda x, y: np.add(x, y)).mapValues(lambda x: (np.divide(x, x[-1]))[0:-1]).collect()
		
		#Compute centroids' movements
		newCentroids = np.array([x[1] for x in newCentroidsTmp])#Build ndarray from a list of key-value pairs
		delta = np.linalg.norm(br_centroids.value - newCentroids, axis=1).sum()
		
		#Broadcast new centroids
		br_centroids = sc.broadcast(newCentroids)
		
		#Print obtained centroids
		print("Obtained centroids")
		print(br_centroids.value)
		
		#Check stop conditions
		print('Delta =', delta, 'obtained at iteration', iteration)
		if (iteration >= maxIterations) or (delta < threshold):
			break
		
		iteration += 1
	
	#Iterative part ended
	print('Algorithm terminated after', iteration, 'iterations. Delta value is:', delta)
	
	#delta.saveAsTextFile("spark-test/test_output")

	start_time = time.time()
	print("Done in %s seconds" % (time.time() - start_time))

