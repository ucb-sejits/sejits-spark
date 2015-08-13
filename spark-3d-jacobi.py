import numpy as np
import math
import random
import sys
import time
from pyspark import SparkContext
from pyspark.conf import SparkConf
from JacobiNd import JacobiNd


"""SETUP"""
start_time = time.time()
length = 100
width = 100
depth = 100
ITERATIONS = 7
true_length = length+2*ITERATIONS
true_width = width+2*ITERATIONS
true_depth = depth+2*ITERATIONS
# 3-D Partition numbers should preferably be perfect cubes
threeD_partitions = 64
threeD_array_data = np.random.random(size = (true_depth,true_length,true_width))


"""SPARK CONTEXT"""
# Number of cores depends on personal machine or EC2
sc = SparkContext("local[8]")
sc.setLogLevel("WARN")


"""CALCULATIONS"""
# If this is able to be cube rooted to an integer, a proper partition can be created
threeD_partition_dimension = round(math.pow(length*width*depth/threeD_partitions,1.0/3.0),0)
end_time = time.time()
print "Setup time: ", end_time-start_time


"""3-D Stencil"""
# Works with backend of python
start_time = time.time()
data4 = threeD_array_data
threeD_stencil = JacobiNd(
			dimensions = 3,
			radius = 1,
			backend = 'python')
for i in range(ITERATIONS):
	data4 = threeD_stencil(data4)
end_time = time.time()
print "Single 3-D run value at 8,8,8: ", data4[8][8][8]
print "Total single 3-D run time: ", end_time-start_time


"""3-D JacobiGhostCellSpark"""
start_time = time.time()
data5 = threeD_array_data
partitioned_threeD_data = np.empty([threeD_partitions,threeD_partition_dimension+2*ITERATIONS,threeD_partition_dimension+2*ITERATIONS
									,threeD_partition_dimension+2*ITERATIONS])
left_bound, top_bound, front_bound = 0, 0, 0
for i in range(1,threeD_partitions+1):
	right_bound = left_bound+threeD_partition_dimension+2*ITERATIONS
	bottom_bound = top_bound+threeD_partition_dimension+2*ITERATIONS
	back_bound = front_bound+threeD_partition_dimension+2*ITERATIONS
	partitioned_threeD_data[i-1] = data5[front_bound:back_bound,top_bound:bottom_bound,left_bound:right_bound]
	if right_bound == width+2*ITERATIONS:
		if bottom_bound == length+2*ITERATIONS:
			left_bound, top_bound = 0, 0
			front_bound = front_bound+threeD_partition_dimension
		else:
			left_bound = 0
			top_bound = top_bound+threeD_partition_dimension
	else:
		left_bound = left_bound+threeD_partition_dimension
threeD_jacobi_rdd = sc.parallelize(partitioned_threeD_data, threeD_partitions)
for i in range(ITERATIONS):
	threeD_jacobi_rdd = threeD_jacobi_rdd.map(threeD_stencil)
end_time = time.time()
print "3-D Ghost Cell value at 8,8,8: ", threeD_jacobi_rdd.collect()[0][8][8][8]
print "Total 3-D Ghost Cell time: ", end_time-start_time
