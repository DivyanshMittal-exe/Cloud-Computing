all:
	python3 tester.py

test:
	 python3 -m cProfile -o profile_data.cprof  client.py
	 pyprof2calltree -k -i profile_data.cprof
	 kcachegrind profile_data.cprof
	
