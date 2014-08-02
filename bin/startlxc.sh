#!/bin/bash
# use the "jepsen" to permit passwordless access to this script
export CASS=/srv/cal/src/apache-cassandra-2.0.7; 
export VD=/srv/cal/src/voldemort-0.96
export MD=/srv/cal/src/mongodb

function restartlxc() {
	for i in `seq 5`; do 
		echo "n$i"
		echo "stop lxc"
		lxc-stop -n n$i
		echo "start lxc"
		lxc-start -n n$i -d
	done
	echo sleeping
	sleep 10
	echo awake
}

case "$1" in
	cassandra)
		echo "starting cassandra"
		restartlxc
		for i in `seq 5`; do 
			echo "starting $1 on n$i"
			ssh n$i "export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-amd64; cd $CASS && bin/cassandra >> logs/n$i/system.log 2>&1"
		done
	;;

	mongo)
		echo "starting mongo"
		restartlxc
		for i in `seq 5`; do 
			echo "starting $1 on n$i"
			ssh n$i "$MD/bin/mongod -f $MD/config/n$i.conf --fork"
		done
	;;

	riak)
		echo "starting riak"
		restartlxc
		for i in `seq 5`; do 
			echo "starting $1 on n$i"
			export RIAKD=/srv/cal/src/riak-lxc/rel/n$i; 
			export RIAK=$RIAKD/bin/riak; 
			ssh n$i "ulimit -n 65536 ; $RIAK start"; 
		done
	;;

	voldemort)
		echo "starting voldemort"
		restartlxc
		for i in `seq 5`; do 
			echo "starting $1 on n$i"
			ssh n$i "cd $VD; bin/voldemort-server.sh config/n$i > config/n$i/logs/voldemort.log &"
		done
	;;

	stopall)
		echo "shutting down lxc containers"
		for i in `seq 5`; do
			echo n$i
			lxc-stop -n n$i
		done
	;;

	*)
		echo "syntax: $0 {voldemort|cassandra|riak|mongod}"
		exit 1
	;;
esac

ps -ef | grep $1
