#!/bin/bash
function run_platform {
	if [ "$platform" == "mysql" ]
	then
		outfile=$outdir/speed_all_$platform-$count.txt
		echo "starting $count $platform speed tests saving output to $outfile"
		speed.py $platform $count | tee $outfile
	else
		startcs.sh $platform 
		echo sleeping $initwait seconds
		sleep $initwait
		outfile=$outdir/speed_all_$platform-$count.txt
		echo "starting $count $platform speed tests saving output to $outfile"
		speed.py $platform $count | tee $outfile
		echo "finished tests"
		stopcs.sh $platform
	fi
}

count=$1
if [ "$count" = "" ]
then
	count=10
fi

initwait=$3
if [ "$initwait" = "" ]
then
	initwait=10
fi

day=`date +%Y%m%d`
outdir=$HOME/public_html/results/$day
mkdir -p $outdir 

platform=$2
if [ "$platform" == "" ] || [ "$platform" == "all" ]
then
	for platform in 'mongo' 'riak' 'voldemort' 'cassandra' 'hbase' 'mysql'
	do
		run_platform
	done

else
	run_platform
fi

