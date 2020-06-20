import getopt
import sys
import time
import numpy as np
from pyspark import SparkContext

import subprocess

#points = 10
#dimension = 2
#filename = 'output.txt'

#Default parameters
maxIterations = 10
threshold = 0.5

def assignToCentroid(pointString, centroids):
	#Convert pointString to float np.array
	point = np.fromstring(pointString, dtype=float, sep=' ')
	
	#Get the index of the centroid the point belongs to
	index = np.linalg.norm(centroids - point, axis=1).argmin()
	
	#Append 1 at the end to be used as counter (of points) in reduce
	return index, np.append(point, [1])

if __name__ == "__main__":
	options, remainder = getopt.getopt(sys.argv[1:], 'o:n:d:i:t:', ['output=', 'num_points=', 'dimension=', 'iterations=', 'threshold='])
	print('OPTIONS :', options)
	for opt, arg in options:
		if opt in ('-o', '--output'):
			filename = arg
		elif opt in ('-n', '--num_points'):
			points = int(arg)
		elif opt in ('-d', '--dimension'):
			dimension = int(arg)
		elif opt in ('-i', '--iterations'):
			maxIterations = int(arg)
		elif opt in ('-t', '--threshold'):
			threshold = float(arg)

	master = "local"
	sc = SparkContext(master, "k-Means")
	
	#Elimino i risultati dell'esecuzione precedente
	subprocess.call(["hadoop", "fs", "-rm", "-r", "spark-test/test_output"])
	
	#Carico i centroidi (versione di test)
	centroids = np.array([[0,0],[1,0],[0,1],[1,1]])
	br_centroids = sc.broadcast(centroids)
	#Fine carico i centroidi
	
	pointStrings = sc.textFile("spark-test/points_10x2.txt_test")
	
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

