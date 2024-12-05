in cmd C:\apache-cassandra-3.11.9\bin
	- cassandra   for starting the server
	- cqlsh       for querying data(USE diabet_data; SELECT * FROM measurements;...)

DATA GENERATOR (generates and saves data to cassandra each 3 seconds)
	py data_generator.py


DATA ANALYZER (plots real time data)
	py real_time_analyzer.py
	http://127.0.0.1:8050/       for dashboard