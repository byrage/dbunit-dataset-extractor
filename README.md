# DBUnitDataSetExtractor

This project will help you to make easy [DBUnit](http://dbunit.sourceforge.net/) DataSet file following your database type and queries.

In First, Fill some properties what if you want to extract in config.properties and Just execute `gradle run` 

If your config is all clear, DataSet xml file is created.


### Properties
1. OUTPUT_FILE_NAME = output file name. extension is XML.
2. TYPE : database type. supporting database type are MySQL and Oracle.
3. HOST = database host
4. PORT = database port
5. DB_NAME = database name
6. ID = database login id
7. PASSWORD = database login password
8. QUERIES = what you want to extract table name and query. if you want to extract all rows in table, input table name only.
- All Tables = '*'
- Table and Query Separator = '-'
- Multiple Table Separator = '/'

### Example
```
OUTPUT_FILE_NAME=output
TYPE=mysql
HOST=localhost
PORT=3306
DB_NAME=test_db
ID=byrage
PASSWORD=password
QUERIES=\
  user - select * from user where id < 10 / \
  program - select * from program where id = 5 / \
  manager
```
