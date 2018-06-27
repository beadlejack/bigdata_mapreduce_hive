# Project 1 - Querying Movie Review Data with MapReduce and Hive

COEN 242 - Big Data 

Santa Clara University 

Spring 2018



### Authors 

Immanuel Amirtharaj, Jackson Beadle

Last edited: May 21, 2018


### Description 

This repo contains the code for querying movie review data for popularity
using MapReduce and Hive. The small dataset is provided. The outputs produced
by both MapReduce and Hive are attached. The report PDF describes how to run
the MapReduce JAR files located in the 'java' folder. The Hive queries, as well
as the other statements to create and populate the Hive tables. 

The Intellij IDE project files for both MapReduce queries are in the 'java' folder
as well as just the Java source files. 

The first query sorts movies by their popularity, as defined by the number of 
reviews they received. The results are sorted in ascending order in the format:
<reviewCount, movieTitle>. The second query finds movies with more than 10 reviews
and an average rating greater than 4.0. The results are sorted in ascending order
by the average rating and are in the format <movieTitle, averageRating, reviewCount>.

These queries were later written as Spark RDD applications. See how the runtime 
compared between the different frameworks at [this repo](https://github.com/beadlejack/bigdata_spark). 
