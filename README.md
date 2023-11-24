# env
The blocks below are the env settings for the modules 

## mysql
```bash
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=dbname
DB_MAX_OPEN=10
DB_MAX_IDLE=10
DB_LIFE_TIME=120
DB_IDLE_TIME=90

DB_INIT_USER=user
DB_INIT_PASSWORD=password
DB_INIT_PARAMS=parseTime\=true&multiStatements\=true

DB_USER=user
DB_PASSWORD=password
DB_PARAMS=parseTime\=true
```

## arguments
```bash
$GOBIN/project -initmysql
```
