## CASSANDRA 
	- cassandra is installed in C:\apache-cassandra-3.11.9
	- to run:
		- run command line in C:\apache-cassandra-3.11.9\bin and then in cmd:
			- 'cassandra'   for starting the server
			- 'cqlsh'  for querying data (CREATE KEYSPACE diabet_data...)
										( USE diabet_data; SELECT * FROM measurements;...)

## DATA GENERATOR 
	- generates random data each 3 seconds and saves it to cassandra table
	- to run: python/py data_generator.py


## DATA ANALYZER
	- plots real time data from cassandra
	- to run: 
		python/py real_time_analyzer.py
	 	and  for dashboard in browser: http://127.0.0.1:8050/      