LOG_LEVEL=NO_LOG
OUT_FILE=out

# Test a specific name.
name=TestInitialElection2A

n=5

test:
	export LOG_LEVEL=${LOG_LEVEL}
	go test -v -test.run ${name} -race | tee ${OUT_FILE}

2A:
	make test name=2A

2B:
	make test name=2B

test-many:
	for i in {1..n}; do time make test; done


