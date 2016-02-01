#!/usr/bin/env bash
rm TopAirlines.jar
rm build/*
hadoop fs -rm -r /user/ec2-user/task1/output
javac -cp .:$(hbase classpath):$(hadoop classpath) -d build TopAirlines.java 
jar -cvf TopAirlines.jar -C build/ ./
yarn jar TopAirlines.jar TopAirlines /user/ec2-user/task1/input /user/ec2-user/task1/output
