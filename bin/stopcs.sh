#!/bin/bash
export CASS=/srv/cal/src/apache-cassandra-2.0.7; 
export VD=/srv/cal/src/voldemort-0.96
export MD=/srv/cal/src/mongodb
export HD=/srv/cal/src/hadoop-1.2.1
export HB=/srv/cal/src/hbase-0.98.3-hadoop1
export RIAKD=/home/cal/riak
export RIAK=$RIAKD/bin/riak; 

case "$1" in
	cassandra)
		echo "stopping cassandra"
		for i in `seq 5`; do 
			echo "stopping $1 on cloudsmall$i"
			ssh cloudsmall$i "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64; cd $CASS && sudo bin/stop-server"
		done
	;;

	mongo)
		echo "stopping mongo"
		for i in `seq 5`; do 
			echo "stopping $1 on cloudsmall$i"
			ssh cloudsmall$i "sudo $MD/bin/mongod -f $MD/config/cs$i.conf --shutdown"
		done
	;;

	riak)
		echo "stopping riak"
		for i in `seq 5`; do 
			echo "stopping $1 on cloudsmall$i"
			ssh cloudsmall$i "sudo $RIAK stop; sudo killall epmd"; 
		done
	;;

	voldemort)
		echo "stopping voldemort"
		for i in `seq 5`; do 
			echo "stopping $1 on cloudsmall$i"
			ssh cloudsmall$i "cd $VD; sudo bin/voldemort-stop.sh"
		done
	;;

	hbase)
		echo "stopping hbase"
		# run as the "cal" user not root
		ssh cloudsmall1 "$HB/bin/hbase rest stop -p 8070"
		# the above didn't seem to work so doing this to make sure it is dead
		ssh cloudsmall1 'pid=`jps | grep RESTServer | cut -f1 -d" "`; echo killing $pid; kill $pid'
		ssh cloudsmall1 "$HB/bin/stop-hbase.sh && $HD/bin/stop-dfs.sh"
		for i in `seq 5`; do
			echo "java processes running on cloudsmall$i"
			ssh cloudsmall$i "jps"
		done
	;;

	*)
		echo "syntax: $0 {voldemort|cassandra|riak|mongod|hbase}"
		exit 1
	;;
esac

