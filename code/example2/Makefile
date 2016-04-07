build:
	javac -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar ZkConnector.java Test.java
run:
	java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:../src/. -Dlog4j.configuration=file://$HOME/myzk/conf/log4j.properties Test
clean:
	rm -f *.class
