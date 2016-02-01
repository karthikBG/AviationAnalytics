#!/usr/bin/env bash
rm TopAirlinesByRoute.jar
rm build/*
hadoop fs -rm -r /user/ec2-user/task1/output
javac -cp .:$(hbase classpath):$(hadoop classpath) -d build TopAirlinesByRoute.java 
jar -cvf TopAirlinesByRoute.jar -C build/ ./
yarn jar TopAirlinesByRoute.jar TopAirlinesByRoute /user/ec2-user/task1/input /user/ec2-user/task1/output
