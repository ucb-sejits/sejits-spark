import numpy as np
import math
import random
import sys
import time
from pyspark import SparkContext
from pyspark.conf import SparkConf
from JacobiNd import JacobiNd

# Current restriction is that the data is square
# Different shaped matrix partitioning, requires calculations as to number of partitions


"""SETUP"""
start_time = time.time()
length = 100
width = 100
ITERATIONS = 30
true_length = length+2*ITERATIONS
true_width = width+2*ITERATIONS
# 2-D Partition numbers should preferably be perfect squares
twoD_partitions = 4
twoD_array_data = np.random.random(size = (true_length,true_width))


"""SPARK CONTEXT"""
# Number of cores depends on personal machine or EC2
sc = SparkContext("local[8]")
sc.setLogLevel("WARN")


"""Calculations"""
# If this is able to be square rooted to an integer, a proper partition can be created
twoD_partition_dimension = round(math.sqrt(length*width/twoD_partitions),0)
# print "2-D Partition Dimension:", twoD_partition_dimension
end_time = time.time()
print "Setup time: ", end_time-start_time


"""2-D Stencil"""
# Works with backend of python
start_time = time.time()
data2 = twoD_array_data
twoD_stencil = JacobiNd(
			dimensions = 2,
			radius = 1,
			backend = 'python')
for i in range(ITERATIONS):
	data2 = twoD_stencil(data2)
end_time = time.time()
print "Single 2-D run value at 35,35: ", data2[35][35]
print "Total single 2-D run time: ", end_time-start_time


"""2-D JacobiGhostCellSpark"""
start_time = time.time()
def helper(data):
	twoD_stencil = JacobiNd(
			dimensions = 2,
			radius = 1,
			backend = 'c')
	return twoD_stencil(data)
data3 = twoD_array_data
partitioned_twoD_data = np.empty([twoD_partitions,twoD_partition_dimension+2*ITERATIONS,twoD_partition_dimension+2*ITERATIONS])
left_bound, top_bound = 0, 0
for i in range(1,twoD_partitions+1):
	right_bound = left_bound+twoD_partition_dimension+2*ITERATIONS
	bottom_bound = top_bound+twoD_partition_dimension+2*ITERATIONS
	partitioned_twoD_data[i-1] = data3[left_bound:right_bound,top_bound:bottom_bound]
	if right_bound == width+2*ITERATIONS:
		left_bound = 0
		top_bound = top_bound+twoD_partition_dimension
	else:
		left_bound = left_bound+twoD_partition_dimension
twoD_jacobi_rdd = sc.parallelize(partitioned_twoD_data, twoD_partitions)
for i in range(ITERATIONS):
	twoD_jacobi_rdd = twoD_jacobi_rdd.map(lambda x: helper(x))
end_time = time.time()
print "2-D Ghost Cell value at 35,35: ", twoD_jacobi_rdd.collect()[0][35][35]
print "Total 2-D Ghost Cell time: ", end_time-start_time
