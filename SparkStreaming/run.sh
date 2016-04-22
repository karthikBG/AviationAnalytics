#!/bin/bash
if [ "$1" == "1.1" ]
then
    spark-submit --master "local[1]" --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 1.1.TopAirports.py
elif [ "$1" == "1.2" ]
then
    spark-submit --master yarn-cluster --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 1.2.TopAirlines.py
elif [ "$1" == "2.1" ]
then
    spark-submit --master yarn-cluster  --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 2.1.TopAirlinesByAirport.py
elif [ "$1" == "2.2" ]
then
    spark-submit --master yarn-cluster --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 2.2.TopDestinationsByAirport.py
elif [ "$1" == "2.3" ]
then
    spark-submit --master yarn-cluster --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 2.3.TopAirlinesByRoute.py
elif [ "$1" == "3.2" ]
then
    spark-submit --master yarn-cluster --jars spark-streaming-kafka-assembly_2.10-1.5.2.jar --num-executors 11 --driver-memory 1g --executor-memory 10g --executor-cores 4 3.2.TomsFlight.py
else
    echo "Invalid option"
fi
