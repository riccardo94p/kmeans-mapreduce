import getopt
import sys
import time
import numpy as np
from pyspark import SparkContext

import subprocess

#points = 10
#dimension = 2
#filename = 'output.txt'

def assignToCentroid(pointString, centroids):
	#convert pointString to float np.array
	point = np.fromstring(pointString, dtype=float, sep=' ')
	
	#get the index of the centroid the point belongs to
	index = np.linalg.norm(centroids - point, axis=1).argmin()
	
	return index, point

def parsePoint(row):
	tokens = row.split(' ')
	return np.array(tokens[:-1], dtype=float), float(tokens[-1])

if __name__ == "__main__":
	options, remainder = getopt.getopt(sys.argv[1:], 'o:n:d:', ['output=', 'num_points=', 'dimension=',])
	print('OPTIONS :', options)
	for opt, arg in options:
		if opt in ('-o', '--output'):
			filename = arg
		elif opt in ('-n', '--num_points'):
			points = int(arg)
		elif opt in ('-d', '--dimension'):
			dimension = int(arg)

	master = "local"
	sc = SparkContext(master, "k-Means")
	
	#Elimino i risultati dell'esecuzione precedente
	subprocess.call(["hadoop", "fs", "-rm", "-r", "spark-test/test_output"])
	
	#Carico i centroidi (versione di test)
	centroids = np.array([[0,0],[1,0],[0,1],[1,1]])
	#Fine carico i centroidi
	

	pointStrings = sc.textFile("spark-test/points_10x2.txt")
	res = pointStrings.map(lambda x: assignToCentroid(x, centroids))
	
	res.saveAsTextFile("spark-test/test_output")

	start_time = time.time()
	print("Done in %s seconds" % (time.time() - start_time))

