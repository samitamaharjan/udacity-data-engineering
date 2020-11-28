# Project Description:
Since Sparkify is getting more and more users, they wanted to migrate on-premise infrastructure to the AWS cloud. Their user activities data - JSON logs and songs were stored in S3. In this project, an ETL pipeline was built where data was loaded to staging tables on Redshift database from S3 and executed SQL queries to create analytics table from those staging tables.

# Prerequisite:
1. Python 3.x.
2. AWS account should be created. S3 bucket and Redshift cluster should be created and the cluster need to be up and running.
3. Jupyter Notebook is optional. This is to understand and the test your logic of the project. 

# How to run it?
1. Create dwh.cfg configuration file to connect AWS services.
2. Launch create_tables.py to set up the database and tables
3. Run etl.py to wrangle the data
4. Check data in the tables using Redshift's query-editor on AWS or any data visualizer. This step is to verify the process.