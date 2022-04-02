# [Udacity Spark Course](https://www.udacity.com/course/learn-spark-at-udacity--ud2002)

## The Power of Spark
Spark is currently one of the most popular tools for big data analytics. You might have heard of other tools such as Hadoop. Hadoop is a slightly older technology although still in use by some companies. Spark is generally faster than Hadoop, which is why Spark has become more popular over the last few years.

![pic](images/key_ratios_review.png)

Distributed vs Parallel computing:
- In general, parallel computing implies multiple CPUs share the same memory.
- With distributed computing, each CPU has its own memory.
- In distributed computing, each computer/machine is connected to the other machines across a network.

![pic](images/distributed_vs_parallel.png)

Hadoop Vocabulary:
- **Hadoop** - an ecosystem of tools for big data storage and data analysis.
- **Hadoop MapReduce** - a system for processing and analysing **large datasets in parallel**. Hadoop is an older system than Spark but is still used by many companies. The major difference between Spark and Hadoop is how they use memory. Hadoop writes intermediate results to disk whereas Spark tries to keep data in memory whenever possible. This makes Spark faster for many use cases.
- **Hadoop Yarn** - a **resource manager** that schedules jobs across a cluster. The manager keeps track of what computer resources to specific tasks.
- **Hadoop Distributed File System (HDFS)** - a big **data storage system** that splits data into chunks and stores the chunks across a cluster of computers.
- **Apache Pig** - a SQL-like language that runs on top of Hadoop MapReduce
- **Apache Hive** - another SQL-like interface that runs on top of Hadoop MapReduce

![pic](images/hadoop_framework.png)

Spark has a streaming library called Spark Streaming although it is not as popular and fast as some other streaming libraries. Other popular streaming libraries include Storm and Flink.

MapReduce:
- **MapReduce** is a programming technique for manipulating large data sets. "Hadoop MapReduce" is a specific implementation of this programming technique. The technique works by first dividing up a large dataset and distributing the data across a cluster. 
    - In the **map** step, each data is analyzed and converted into a (key, value) pair. 
    - Then these key-value pairs are **shuffled** across the cluster so that all keys are on the same machine. 
    - In the **reduce** step, the values with the same keys are combined together.

![pic](images/mapreduce.png)

## Data Wrangling with Spark
## Setting up Spark Clusters with AWS
## Debugging and Optimisation
## Machine Learning with Spark