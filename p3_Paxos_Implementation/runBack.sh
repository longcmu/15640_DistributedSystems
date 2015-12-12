NODE_PORT0=$((9001))
NODE_PORT1=$((9002))
NODE_PORT2=$((9003))
PAXOS_APP=$GOPATH/bin/main
PAXOS_NODE=$GOPATH/bin/prunner
ALL_PORTS="${NODE_PORT0},${NODE_PORT1},${NODE_PORT2}"

# Build the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440-F15/paxosapp/runners/prunner
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Build the test binary to use to test the student's paxos node implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440-F15/paxosapp/main
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

##################################################

# Start paxos node.
${PAXOS_NODE} -ports=${ALL_PORTS} -N=3 -id=0 2> /dev/null &
PAXOS_NODE_PID1=$!
sleep 1

${PAXOS_NODE} -ports=${ALL_PORTS} -N=3 -id=1 2> /dev/null &
PAXOS_NODE_PID2=$!
sleep 1

${PAXOS_NODE} -ports=${ALL_PORTS} -N=3 -id=2 2> /dev/null &
PAXOS_NODE_PID3=$!
sleep 1

# Start paxosapp. 
${PAXOS_APP}

# Kill paxos node.
kill -9 ${PAXOS_NODE_PID1}
kill -9 ${PAXOS_NODE_PID2}
kill -9 ${PAXOS_NODE_PID3}
wait ${PAXOS_NODE_PID1} 2> /dev/null
wait ${PAXOS_NODE_PID2} 2> /dev/null
wait ${PAXOS_NODE_PID3} 2> /dev/null