.PHONY: all clean

all:
	mvn clean package
	scp target/*.jar hadoop@172.16.1.154:~
