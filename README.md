## YCSB 

More about YCSB tool read [here](https://github.com/brianfrankcooper/YCSB/tree/master/jdbc).

### 1. Postgres (one node)
```sql
CREATE DATABASE ycsb;

\c ycsb;

CREATE TABLE usertable (
  YCSB_KEY VARCHAR(255) PRIMARY KEY,
  FIELD0 TEXT, FIELD1 TEXT,
  FIELD2 TEXT, FIELD3 TEXT,
  FIELD4 TEXT, FIELD5 TEXT,
  FIELD6 TEXT, FIELD7 TEXT,
  FIELD8 TEXT, FIELD9 TEXT
);
```

### 1. Citus

#### 1.1 Run on each node
```sql
CREATE DATABASE ycsb;

\c ycsb;

CREATE EXTENSION citus;

CREATE TABLE usertable (
  YCSB_KEY VARCHAR(255) PRIMARY KEY,
  FIELD0 TEXT, FIELD1 TEXT,
  FIELD2 TEXT, FIELD3 TEXT,
  FIELD4 TEXT, FIELD5 TEXT,
  FIELD6 TEXT, FIELD7 TEXT,
  FIELD8 TEXT, FIELD9 TEXT
);
```

#### 1.2 Run only on master node
```sql
SELECT * from master_add_node('192.168.1.11', 5432);
SELECT * from master_add_node('192.168.1.13', 5432);

SELECT * FROM master_get_active_worker_nodes();

SELECT create_distributed_table('usertable', 'ycsb_key');
```

### 2. CockroachDB (run on any node)
```sql
CREATE DATABASE ycsb;

USE ycsb;

CREATE TABLE usertable (
  YCSB_KEY VARCHAR(255) PRIMARY KEY,
  FIELD0 TEXT, FIELD1 TEXT,
  FIELD2 TEXT, FIELD3 TEXT,
  FIELD4 TEXT, FIELD5 TEXT,
  FIELD6 TEXT, FIELD7 TEXT,
  FIELD8 TEXT, FIELD9 TEXT
);
```

### Load data 
5M records is about 6.4 GB on single Postgres database.

```bash
nohup $(YCSB_HOME)/bin/ycsb load jdbc -P $(pwd)/workloads/workloada -P $(pwd)/configs/<db-config.properties> -p threadcount=16 -p recordcount=5000000 -s > loada.log 2>&1 &
nohup $(YCSB_HOME)/bin/ycsb load jdbc -P $(pwd)/workloads/workloade -P $(pwd)/configs/<db-config.properties> -p threadcount=16 -p recordcount=5000000 -s > loadb.log 2>&1 &
```
