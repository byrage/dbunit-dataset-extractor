# DbUnitDataSetExtractor

1. project description
2. config.properties description and guide

Just execute `gradle run`. 

if your config is all clear, DataSet xml file is created as you type.

##### example 
```
OUTPUT_FILE_NAME=output
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
```
