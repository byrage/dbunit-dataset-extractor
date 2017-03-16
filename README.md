# DbUnitDataSetExtractor

This project is help you to make easy [DBUnit](http://dbunit.sourceforge.net/) DataSet file following your database type and queries.

Fill some properties in config.properties and Just execute `gradle run` 

If your config is all clear, DataSet xml file is created.

##### Properties
1. TYPE : database type. supporting database type are MySQL and Oracle.
2. HOST = database host
3. PORT = database port
4. DB_NAME = database name
5. ID = database login id
6. PASSWORD = database login password
7. TABLES_AND_QUERIES = what you want to extract table name and query. if you want to extract all rows in table, input table name only.
- table and query separator = '-'
- multiple table separator = '/'
8. OUTPUT_FILE_NAME = output file name. extension is XML.

##### Example
```
TYPE=mysql
HOST=localhost
PORT=3306
DB_NAME=database
ID=root
PASSWORD=root
TABLES_AND_QUERIES=\
  user - select * from user where id < 10 / \
  program - select * from program where id = 5 / \
  manager
  OUTPUT_FILE_NAME=output
```
