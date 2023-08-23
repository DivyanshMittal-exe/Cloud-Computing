all:
	python3 tester.py

test:
	cat mylib.lua | redis-cli -x FUNCTION LOAD REPLACE
	 python3 -m cProfile -o profile_data.cprof  client.py
	 pyprof2calltree -k -i profile_data.cprof
	 kcachegrind profile_data.cprof
	
setup:
	# redis-server 
	redis-cli CONFIG SET requirepass pass
	cat mylib.lua | redis-cli -a pass -x FUNCTION LOAD REPLACE
