# hive_udf
UDF, GenericUDF, UDTF, UDAF
#Create a table
Create a table named employee:
```sql
CREATE TABLE employee_1(
	name			STRING,
	salary			FLOAT,
	subordinates	ARRAY<STRING>,
	deductions		MAP<STRING, FLOAT>,
	address			STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
COLLECTION ITEMS TERMINATED BY '\002'
MAP KEYS TERMINATED BY '\003'
LINES TERMINATED BY '\n'
```
Load the data that is in data directory.
```shell
hadoop fs -put employee.txt /hive
```sql
LOAD DATA INPATH '/hive/employees.txt'
OVERWRITE INTO TABLE employee
```
