#!/usr/bin/env bash
rm TopDestinationsByAirport.jar
rm build/*
hadoop fs -rm -r /user/ec2-user/task1/output
javac -cp .:$(hbase classpath):$(hadoop classpath) -d build TopDestinationsByAirport.java 
jar -cvf TopDestinationsByAirport.jar -C build/ ./
yarn jar TopDestinationsByAirport.jar TopDestinationsByAirport /user/ec2-user/task1/input /user/ec2-user/task1/output
