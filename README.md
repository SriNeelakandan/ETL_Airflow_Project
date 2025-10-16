# ETL Hands On Mini Project

I have :

Mysql Server -> sample (database) -> sales(table)

My task is :

- Extract the data from sales (Table) and stored it as pandas dataframe

- Apply transformations : - drop null records, create new columns (Total,Amount_paid)

- Load the transformed data as csv file to specific folder (say output) with filename with timestamp (say etl_output_18-48-49)



How I schedule?

- Every 5 minutes the above task should run 

- Created a dag to acheive this scheduling

- Initate my task through shell script file that contains the command to run my python file

- scheduling starts when I turn on my dag in my airflow webserver

- logs are present to understand the execution of my task 



Tech Stack:  Python, Pandas, MySQL, Linux, Apache Airflow
