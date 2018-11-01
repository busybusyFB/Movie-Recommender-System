#!/bin/bash

echo "Starting recommender"

hdfs dfs -mkdir /input
hdfs dfs -put input/* /input

hdfs dfs -rm -r /dataDividedByUser
hdfs dfs -rm -r /coOccurrenceMatrix
hdfs dfs -rm -r /Normalize
hdfs dfs -rm -r /Multiplication
hdfs dfs -rm -r /Sum

cd src/main/java/

hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class

hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum

rm -r *.class *.jar

hdfs dfs -cat /Sum/*
