rm TopAirports.jar
rm build/*
hadoop fs -rm -r /user/ec2-user/task1/output
javac -cp .:$(hbase classpath):$(hadoop classpath) -d build TopAirports.java 
jar -cvf TopAirports.jar -C build/ ./
yarn jar TopAirports.jar TopAirports /user/ec2-user/task1/input /user/ec2-user/task1/output
