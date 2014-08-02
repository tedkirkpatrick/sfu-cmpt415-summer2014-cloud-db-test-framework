#!/bin/bash
export CASS=/srv/cal/src/apache-cassandra-2.0.7; 
export VD=/srv/cal/src/voldemort-0.96
export MD=/srv/cal/src/mongodb
export HD=/srv/cal/src/hadoop-1.2.1
export HB=/srv/cal/src/hbase-0.98.3-hadoop1
export RIAKD=/home/cal/riak; 
export RIAK=$RIAKD/bin/riak; 

echo "attempting to start $1"
case "$1" in
	cassandra)
		for i in `seq 5`; do 
			echo "starting $1 on cloudsmall$i"
			ssh cloudsmall$i "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64; cd $CASS && sudo bin/cassandra -p /var/run/cassandra >> logs/cloudsmall$i/system.log 2>&1"
			ssh cloudsmall$i "ps -ef | grep $1 | grep -v grep"
		done
	;;

	mongo)
		for i in `seq 5`; do 
			echo "starting $1 on cloudsmall$i"
			ssh cloudsmall$i "sudo $MD/bin/mongod -f $MD/config/cs$i.conf --fork"
			ssh cloudsmall$i "ps -ef | grep $1 | grep -v grep"
		done
	;;

	riak)
		for i in `seq 5`; do 
			echo "starting $1 on cloudsmall$i"
			ssh cloudsmall$i "sudo $RIAK start"; 
			ssh cloudsmall$i "ps -ef | grep $1 | grep -v grep"
		done
	;;

	voldemort)
		for i in `seq 5`; do 
			echo "starting $1 on cloudsmall$i"
			ssh cloudsmall$i "cd $VD; sudo bin/voldemort-server.sh /home/cal/voldemort > /home/cal/voldemort/logs/voldemort.log &"
			ssh cloudsmall$i "ps -ef | grep $1 | grep -v grep"
		done
	;;

	hbase)
		# run as the "cal" user not root
		ssh cloudsmall1 "$HD/bin/start-dfs.sh && $HB/bin/start-hbase.sh"
		ssh cloudsmall1 "$HB/bin/hbase rest start -p 8070 > /home/cal/hbase/logs/hbase-cal-rest.log 2>&1 &"
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

