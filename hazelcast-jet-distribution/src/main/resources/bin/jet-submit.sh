#!/bin/sh

SCRIPT_DIR=`dirname "$0"`
. $SCRIPT_DIR/common.sh

JAR_FILE=$1
shift
$JAVA -cp $CLASSPATH:$JAR_FILE com.hazelcast.jet.server.JetBootstrap $JAR_FILE $@
