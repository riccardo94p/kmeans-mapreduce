import getopt
import sys
import time
import subprocess
import numpy as np
from pyspark import SparkContext

#Default parameters
dimension = 0
maxIterations = 10
threshold = 0.5
k = 4
seed = 1
master = "local"
inputPath = "Resources/Input/points_1000x7.txt"
outputPath = "Resources/Output/Spark"

def assignToCentroid(pointString, centroids):
	#Convert pointString to float np.array
	point = np.fromstring(pointString, count=dimension, sep=' ')
	
	#Get the index of the centroid the point belongs to
	index = np.linalg.norm(centroids - point, axis=1).argmin()
	
	#Append 1 at the end to be used as counter (of points) in reduce
	return index, np.append(point, [1])

if __name__ == "__main__":#TODO: testare se il parsing dei parametri funziona
	options, remainder = getopt.getopt(sys.argv[1:], 'o:d:k:j:t:s:m:', ['input=', 'output=', 'dimension=', 'num_k=', 'iterations=', 'threshold=', 'seed=', 'master='])
	print('OPTIONS :', options)
	for opt, arg in options:
		if opt in ('-i', '--input'):
			inputPath = arg
		elif opt in ('-o', '--output'):
			outputPath = arg
		elif opt in ('-d', '--dimension'):
			dimension = int(arg)
		elif opt in ('-k', '--num_k'):
			k = int(arg)
		elif opt in ('-j', '--iterations'):
			maxIterations = int(arg)
		elif opt in ('-t', '--threshold'):
			threshold = float(arg)
		elif opt in ('-s', '--seed'):
			seed = int(arg)
		elif opt in ('-m', '--master'):
			master = arg

	start_time = time.time()
	sc = SparkContext(master, "k-Means")
	
	#Clean output directory
	subprocess.call(["hadoop", "fs", "-rm", "-r", outputPath])
	
	#Load points from file in HDFS
	pointStrings = sc.textFile(inputPath)
	
	#Get the default points' dimension
	if dimension == 0:
		aPoint = pointStrings.takeSample(False, 1, seed)
		dimension = np.fromstring(aPoint[0], sep=' ').shape[0]
	
	#Select k random start points
	sample = pointStrings.takeSample(False, k, seed)
	centroids = np.array([np.fromstring(x, count=dimension, sep=' ') for x in sample])
	br_centroids = sc.broadcast(centroids)
	
	
	iteration = 1
	delta = float("inf")
	
	while True:
		#Perform map-reduce
		centroidPointPairs = pointStrings.map(lambda x: assignToCentroid(x, br_centroids.value))
		newCentroidsRDD = centroidPointPairs.reduceByKey(lambda x, y: np.add(x, y)).mapValues(lambda x: (np.divide(x, x[-1]))[0:-1])
		
		#Compute centroids' movements
		newCentroids = np.array([x[1] for x in newCentroidsRDD.collect()])#Build ndarray from a list of key-value pairs
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
	newCentroidsRDD.saveAsTextFile(outputPath)
	print('Algorithm terminated in', (time.time() - start_time), 'seconds after', iteration, 'iterations. Delta is:', delta)

