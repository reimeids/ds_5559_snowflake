USE ROLE ACCOUNTADMIN;
USE DATABASE DS5559_DATA;
USE SCHEMA PUBLIC;

CREATE OR REPLACE PROCEDURE column_average(tableName VARCHAR, sl VARCHAR, 
                                          sw VARCHAR, pl VARCHAR, pw VARCHAR)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
packages = ('snowflake-snowpark-python')
HANDLER = 'column_average'
AS
$$
from snowflake.snowpark.functions import avg, col

def column_average(session, table_name, sl, sw, pl, pw):
    df = session.table(table_name)
    return df.agg(avg(col(sl)), avg(col(sw)), avg(col(pl)), avg(col(pw)))
$$; 

SHOW PROCEDURES; 

CALL column_average('IRIS_DATA', 'SEPAL_LENGTH', 'SEPAL_WIDTH', 'PETAL_LENGTH', 'PETAL_WIDTH');
