import numpy as np
import random
import time
import sys
sys.path.append('/home/stephen/spark-1.4.0/python')
sys.path.append('/home/stephen/spark-1.4.0/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext
from pyspark.conf import SparkConf
from itertools import *

sc = SparkContext("local[8]")
sc.setLogLevel("WARN")
ITERATIONS = 1
GHOST_ROW_COUNT = 2 # Number of Inidividual Iterations
PARTITION_NUM = 8

length = 24
width = 24
partition_width = length*width/PARTITION_NUM
GHOST_COUNT = GHOST_ROW_COUNT*width

def partition(pair):
	index = pair[0]
	value = pair[1]
	if index < partition_width-GHOST_COUNT:
		return [(1,(index,value))]
	if index < partition_width+GHOST_COUNT:
		return (1,(index,value)), (2,(index,value))
	if index < 2*partition_width-GHOST_COUNT:
		return [(2,(index,value))]
	if index < 2*partition_width+GHOST_COUNT:
		return (2,(index,value)), (3,(index,value))
	if index < 3*partition_width-GHOST_COUNT:
		return [(3,(index,value))]
	if index < 3*partition_width+GHOST_COUNT:
		return (3,(index,value)), (4,(index,value))
	if index < 4*partition_width-GHOST_COUNT:
		return [(4,(index,value))]
	if index < 4*partition_width+GHOST_COUNT:
		return (4,(index,value)), (5,(index,value))
	if index < 5*partition_width-GHOST_COUNT:
		return [(5,(index,value))]
	if index < 5*partition_width+GHOST_COUNT:
		return (5,(index,value)), (6,(index,value))
	if index < 6*partition_width-GHOST_COUNT:
		return [(6,(index,value))]
	if index < 6*partition_width+GHOST_COUNT:
		return (6,(index,value)), (7,(index,value))
	if index < 7*partition_width-GHOST_COUNT:
		return [(7,(index,value))]
	if index < 7*partition_width+GHOST_COUNT:
		return (7,(index,value)), (8,(index,value))
	return [(8,(index,value))]

def jacobi_partition(iterator):
	for elem in iterator:
		return elem[0],jacobi_list(elem[1])

def jacobi(pair):
	key = pair[0]
	value = pair[1]
	x = key%width
	y = key//width
	if y == 0:
		if x == 0:
			return ((key,0),(key+width,value),(key,1),(key+1,value))
		if x == width-1:
			return ((key,0),(key+width,value),(key-1,value),(key,1))
		return ((key,0),(key+width,value),(key-1,value),(key+1,value))
	if y == length-1:
		if x == 0:
			return ((key-width,value),(key,0),(key,1),(key+1,value))
		if x == width-1:
			return ((key-width,value),(key,0),(key-1,value),(key,1))
		return ((key-width,value),(key,0),(key-1,value),(key+1,value))
	if x == 0:	
		return ((key-width,value),(key+width,value),(key,1),(key+1,value))
	if x == width-1:
		return ((key-width,value),(key+width,value),(key-1,value),(key,1))
	return ((key-width,value),(key+width,value),(key-1,value),(key+1,value))

start_time = time.time()
# text_data = sc.textFile("TestData.txt")
# transposed_array_data = text_data.flatMap(lambda x: (x.replace('\n',' ').split(' ')))
# file_read_time = time.time()
# array_data = map(int,transposed_array_data.collect())
array_data = np.random.random(size = (length,width))
print array_data
jacobi_rdd = sc.parallelize(array_data,8)
print jacobi_rdd.getNumPartitions()
print jacobi_rdd.glom().collect()
# print array_data
jacobi_rdd = sc.parallelize(zip(range(len(array_data)),array_data),8)	
jacobi_rdd = jacobi_rdd.flatMap(partition).groupByKey()
jacobi_rdd = jacobi_rdd.mapValues(list)
# print jacobi_rdd.collect()
# zip_time = time.time()
# map_reduce_time = 0
# divide_time = 0
for i in range(ITERATIONS):
	# map_reduce_start = time.time()
	jacobi_rdd = jacobi_rdd.mapPartitions(jacobi_partition)
	# jacobi_rdd = jacobi_rdd.mapPartitions(jacobi) # .reduceByKey(lambda x,y: float(x+y))
	print jacobi_rdd.collect()
	# map_reduce_time = map_reduce_time - map_reduce_start + time.time()
	# divide_time_start = time.time()
	jacobi_rdd = jacobi_rdd.map(lambda x: (int(x[0]),x[1]/4))
	# divide_time = divide_time - divide_time_start + time.time()
# jacobi_rdd.values().saveAsTextFile("Jacobi_Spark_Results.txt")
end_time = time.time()
# jacobi_rdd = jacobi_rdd.sortByKey()
# print jacobi_rdd.collect()
# print "FileRead",file_read_time-start_time
# print "Zip",zip_time-file_read_time
# print "Iteration",end_time-zip_time
# print "Map Reduce",map_reduce_time
# print "Divide",divide_time
print "Total time",end_time-start_time
