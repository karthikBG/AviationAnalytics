#!/usr/bin/env bash
rm TopAirlinesByAirport.jar
rm build/*
hadoop fs -rm -r /user/ec2-user/task1/output
javac -cp .:$(hbase classpath):$(hadoop classpath) -d build TopAirlinesByAirport.java 
jar -cvf TopAirlinesByAirport.jar -C build/ ./
yarn jar TopAirlinesByAirport.jar TopAirlinesByAirport /user/ec2-user/task1/input /user/ec2-user/task1/output
