drop table SUPPLIER;
drop table NATION;
drop table REGION;
CREATE TABLE SUPPLIER ( S_SUPPKEY INT ,S_NAME CHAR(25) ,S_ADDRESS VARCHAR(40) ,S_NATIONKEY INT ,S_PHONE CHAR(15) ,S_ACCTBAL FLOAT ,S_COMMENT VARCHAR(101) );
CREATE TABLE NATION ( N_NATIONKEY INT ,N_NAME CHAR(25) ,N_REGIONKEY INT ,N_COMMENT VARCHAR(152));
CREATE TABLE REGION ( R_REGIONKEY INT ,R_NAME CHAR(25) ,R_COMMENT VARCHAR(152));

load ../../tmp/benchmark_data/supplier.tbl into SUPPLIER ;
load ../../tmp/benchmark_data/nation.tbl into NATION ;
load ../../tmp/benchmark_data/region.tbl into REGION ;


SET enable_nestloop = false;
SET enable_sortmerge = false;
SET enable_hashjoin = true;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE N_NATIONKEY = S_SUPPKEY;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE N_NATIONKEY = S_SUPPKEY;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY = 5;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY = 5;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_NAME = 'Supplier#000000229' AND S_NAME = '123';
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_NAME = 'Supplier#000000229' AND S_NAME = '123';
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<30 AND S_SUPPKEY<20 AND S_SUPPKEY<10 AND S_SUPPKEY = 5 ;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<30 AND S_SUPPKEY<20 AND S_SUPPKEY<10 AND S_SUPPKEY = 5 ;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10000 AND S_SUPPKEY<20 AND S_SUPPKEY<10;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10000 AND S_SUPPKEY<20 AND S_SUPPKEY<10;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY>20;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY>20;
set enable_optimizer = false;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY=10 AND S_SUPPKEY=20;
set enable_optimizer = true;
select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY=10 AND S_SUPPKEY=20;


set enable_optimizer = true;
select * from SUPPLIER,NATION,REGION WHERE S_SUPPKEY = N_NATIONKEY AND N_NATIONKEY = R_REGIONKEY AND N_NATIONKEY < 5 AND R_REGIONKEY<10;

set enable_optimizer = false;
select * from SUPPLIER,NATION,REGION WHERE S_SUPPKEY = N_NATIONKEY AND N_NATIONKEY = R_REGIONKEY AND N_NATIONKEY < 5 AND R_REGIONKEY<10;


-- SET enable_nestloop = true;
-- set enable_optimizer = false;
-- select * from SUPPLIER,NATION WHERE N_NATIONKEY = S_SUPPKEY;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY>20;

-- set enable_optimizer = true;
-- select * from SUPPLIER,NATION WHERE N_NATIONKEY = S_SUPPKEY;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10;
-- select * from SUPPLIER,NATION WHERE S_SUPPKEY = N_NATIONKEY AND S_SUPPKEY<10 AND S_SUPPKEY>20;