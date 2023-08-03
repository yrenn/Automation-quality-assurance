# Quality_Assurance & Automation Testing

# background
As test analysts, we should be responsible for the data quality after data migration. In the past, we compare the tables manually to make the data type and value accurate and correct. For saving time and enhancing efficiency, I create the automation testing using a Python script. Hopefully, it can be used in other projects in the future.


## Project design


![flow_chart](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/ace1c11c-9887-4d9f-bd85-225997024492)


## result sample:
![image](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/93b75b42-d2ed-45c6-a425-2154ee519fc7)

#### summary

![image](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/260f67c7-0e41-42f3-8a34-532b36725515)

#### Differences example:
![image](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/4d178c89-9d41-4827-ae61-b67f6d65ff4f)

#### Duplicates
![image](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/3cbbf23c-7c6e-4447-b87a-6bc21b512639)

#### Missing rows
![image](https://github.com/yrenn/QA_data_comparsion_sqlserver_and_snowflake/assets/118937529/03f52961-fb09-463a-8bda-446dca81000e)



## lesson learned
1. As for Python script, the most important is configuration, which means setting Python version and package versions. we should make sure all packages work and without any conflict.
2. Setting proper parameters can reduce bugs and show more features. For example, defining the primary key before comparing and expanding differences from 10 to 100 to show all differences in the report can be more accurate and clear.
3. Using metafile to control the tables and filter will make the script reusable in other conditions. Users can modify the parameters in Metafile and create the SQL queries by themselves to achieve their goals.

