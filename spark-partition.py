import numpy as np
import math
import random
from pyspark import SparkContext
from pyspark.conf import SparkConf

# Current restriction is that the data is square
# Different shaped matrix partitioning, requires calculations as to number of partitions

# Number of cores depends on personal machine or EC2
sc = SparkContext("local[8]")
sc.setLogLevel("WARN")

"""SETUP"""
length = 8
width = 8
depth = 8
# 2-D Partition numbers should preferably be perfect squares
twoD_partitions = 4
# 3-D Partition numbers should preferably be perfect cubes
threeD_partitions = 8
ITERATIONS = 2
twoD_array_data = np.random.randint(5, size = (length+2*ITERATIONS,width+2*ITERATIONS))
threeD_array_data = np.random.randint(5, size = (depth+2*ITERATIONS,length+2*ITERATIONS,width+2*ITERATIONS))


"""Calculations"""
# If this is able to be square rooted to an integer, a proper partition can be created
twoD_partition_dimension = round(math.sqrt(length*width/twoD_partitions),0)
# print "2-D Partition Dimension:", twoD_partition_dimension

# If this is able to be cube rooted to an integer, a proper partition can be created
threeD_partition_dimension = round(math.pow(length*width*depth/threeD_partitions,1.0/3.0),0)
# print "2-D Partition Dimension:", threeD_partition_dimension


"""2-D array"""
partitioned_twoD_data = np.empty([twoD_partitions,twoD_partition_dimension+2*ITERATIONS,twoD_partition_dimension+2*ITERATIONS])
left_bound, top_bound = 0, 0
for i in range(1,twoD_partitions+1):
	right_bound = left_bound+twoD_partition_dimension+2*ITERATIONS
	bottom_bound = top_bound+twoD_partition_dimension+2*ITERATIONS
	partitioned_twoD_data[i-1] = twoD_array_data[left_bound:right_bound,top_bound:bottom_bound]
	if right_bound == width+2*ITERATIONS:
		left_bound = 0
		top_bound = top_bound+twoD_partition_dimension
	else:
		left_bound = left_bound+twoD_partition_dimension
twoD_jacobi_rdd = sc.parallelize(partitioned_twoD_data, twoD_partitions)
print twoD_jacobi_rdd.glom().collect()
#Run SEJITS here


"""3-D array"""
partitioned_threeD_data = np.empty([threeD_partitions,threeD_partition_dimension+2*ITERATIONS,threeD_partition_dimension+2*ITERATIONS
									,threeD_partition_dimension+2*ITERATIONS])
left_bound, top_bound, front_bound = 0, 0, 0
for i in range(1,threeD_partitions+1):
	right_bound = left_bound+twoD_partition_dimension+2*ITERATIONS
	bottom_bound = top_bound+twoD_partition_dimension+2*ITERATIONS
	back_bound = front_bound+twoD_partition_dimension+2*ITERATIONS
	partitioned_threeD_data[i-1] = threeD_array_data[front_bound:back_bound,top_bound:bottom_bound,left_bound:right_bound]
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
print threeD_jacobi_rdd.glom().collect()
#Run SEJITS here
