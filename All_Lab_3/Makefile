all: setup
	python3 tester.py

one:
	sleep 1
	python3 tester.py

test: setup	
	 python3 -m cProfile -o profile_data.cprof  client.py
	 pyprof2calltree -k -i profile_data.cprof
	 kcachegrind profile_data.cprof
	
setup:
	# redis-server 
	redis-cli CONFIG SET requirepass pass
	cat mylib.lua | redis-cli -a pass -x FUNCTION LOAD REPLACE

ft: setup
	python3 ft_random_check.py

stress: setup
	for i in $$(seq 1 100); do \
        make ft & make one; \
		wait; \
	done