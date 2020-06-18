.PHONY: all clean

all:
	mvn -q clean package
	scp target/*.jar hadoop@172.16.1.154:~
