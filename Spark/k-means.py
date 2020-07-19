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

def parseStrings(pointString):
	#Convert pointString to float np.array
	return np.fromstring(pointString, count=dimension, sep=' ')

def assignToCentroid(point, centroids):
	#Get the index of the centroid the point belongs to
	index = np.linalg.norm(centroids - point, axis=1).argmin()
	
	#Append 1 at the end to be used as counter (of points) in reduce
	return index, np.append(point, [1])

if __name__ == "__main__":
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
	
	#Get the native points' dimension
	aPoint = pointStrings.takeSample(False, 1, seed)
	nativeDimension = np.fromstring(aPoint[0], sep=' ').shape[0]
	if dimension == 0 or dimension > nativeDimension:
		print('Dimension set to', nativeDimension)
		dimension = nativeDimension
	
	#points_time = (time.time() - start_time)
	
	#start_cache_time = time.time()
	
	#parse points and cache them once
	pointsDRR = pointStrings.map(parseStrings).cache()
	
	#cache_time = (time.time() - start_cache_time)
	
	#start_sample_time = time.time()
	
	#Select k random start points
	centroids = np.array(pointsDRR.takeSample(False, k, seed))
	br_centroids = sc.broadcast(centroids)
	
	iteration = 1
	delta = float("inf")
	
	#sample_time = (time.time() - start_sample_time)
	
	while True:
		#Perform map-reduce
		centroidPointPairs = pointsDRR.map(lambda x: assignToCentroid(x, br_centroids.value))
		newCentroidsRDD = centroidPointPairs.reduceByKey(lambda x, y: np.add(x, y)).mapValues(lambda x: (np.divide(x, x[-1]))[0:-1])
		
		#Compute centroids' movements
		newCentroids = np.array(newCentroidsRDD.sortByKey(ascending=True).values().collect())#Build ndarray from a list of key-value pairs
		delta = np.linalg.norm(br_centroids.value - newCentroids, axis=1).mean()
		
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
	
	elapsed_time = (time.time() - start_time)
	
	print('Algorithm terminated in', elapsed_time, 'seconds after', iteration, 'iterations. Delta is:', delta)
	#print("Points time: ", points_time, "; takeSample time: ", sample_time)
	
	#Iterative part ended
	newCentroidsRDD.saveAsTextFile(outputPath)

	f= open("spark_results.txt","a+")
	resultstr = str(k) + " " + inputPath + " " + str(elapsed_time) + " " + str(iteration) + " | " + str(points_time) + " " + str(cache_time) + " " + str(sample_time)+"\n"
	f.write(resultstr)
	f.close()
