# Airflow Pipelines

Constructing Airflow pipelines to retrieve AWS data, load tables, and check data quality. Completed as part of Udacity's Data Engineering Nanodegree.

The "Dags" folder contains the directed acyclical graphs and tasks used within the graphs.

The "plugins" folder contains operators used within the dags and SQL helper functions.

"Create_tables.sql" contains the SQL script used for loading data into tables.  As the focus of this project was Airflow, this code was provided by Udacity to maintain the project emphasis.

To run, enter "/opt/airflow/start.sh" into terminal to initialize an Airflow GUI to visualize job steps and status.
