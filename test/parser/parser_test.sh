#!/bin/bash
# set -euo pipefail

print_with_color() {
    echo -e "\e[$1m$2\e[0m"
}

print_red(){
    print_with_color 31 "$1"
}

print_green(){
    print_with_color 32 "$1"
}

print_yellow(){
    print_with_color 33 "$1"
}

print_blue(){
    print_with_color 34 "$1"
}

print_white(){
    print_with_color 37 "$1"
}

clean_up(){
    # print_red "执行清理操作"
    ps -aux | grep easydb | grep $(whoami) | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
}

# 退出时调用clean_up函数关闭Server进程
trap clean_up EXIT;

SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname $SCRIPT_PATH)
ROOT_PATH=$(dirname $SCRIPT_PATH | xargs dirname | xargs dirname)
# 服务端和客户端路径（根据实际情况修改）
SERVER_PATH=$ROOT_PATH/build/bin/easydb_server
CLIENT_PATH=$ROOT_PATH/build/bin/easydb_client
SERVE_PORT=18765
# 数据库保存目录
DB_PATH=$SCRIPT_DIR/test.db
SERVER_LOG_PATH=$SCRIPT_DIR/server.log
# 测试数据目录（根据实际情况修改）
DATA_PATH=$ROOT_PATH/tmp/benchmark_data
DATA_SUPPLIER_PATH=$DATA_PATH/supplier.tbl
DATA_CUSTOMER_PATH=$DATA_PATH/customer.tbl
DATA_LINEITEM_PATH=$DATA_PATH/lineitem.tbl
DATA_NATION_PATH=$DATA_PATH/nation.tbl
DATA_ORDERS_PATH=$DATA_PATH/orders.tbl
DATA_PART_PATH=$DATA_PATH/part.tbl
DATA_PARTSUPP_PATH=$DATA_PATH/partsupp.tbl
DATA_REGION_PATH=$DATA_PATH/region.tbl
DATA_TEST_PATH=$DATA_PATH/test.tbl
execute(){
    print_blue "执行命令:"
    echo -e "$1 \n" >> out.sql 
    if [ "$2" != "." ]; then
        echo "$1; exit;" | $CLIENT_PATH -p $SERVE_PORT
    fi
}

execute_quiet(){
    echo "$1; exit;" | $CLIENT_PATH -p $SERVE_PORT > /dev/null 2>&1
}

create_tables(){
    # 创建表
create_table_nation=$(cat <<EOF
CREATE TABLE nation(\
    N_NATIONKEY INTEGER NOT NULL,\
    N_NAME CHAR(25) NOT NULL,\
    N_REGIONKEY INTEGER NOT NULL,\
    N_COMMENT VARCHAR(152)\
);
EOF
)
execute "$create_table_nation"

create_table_region=$(cat <<EOF
CREATE TABLE region(\
    R_REGIONKEY INTEGER NOT NULL,\
    R_NAME CHAR(25) NOT NULL,\
    R_COMMENT VARCHAR(152)\
);
EOF
)

execute "$create_table_region"

create_table_part=$(cat <<EOF
CREATE TABLE part(\
    P_PARTKEY INTEGER NOT NULL,\
    P_NAME VARCHAR(55) NOT NULL,\
    P_MFGR CHAR(25) NOT NULL,\
    P_BRAND CHAR(10) NOT NULL,\
    P_TYPE VARCHAR(25) NOT NULL,\
    P_SIZE INTEGER NOT NULL,\
    P_CONTAINER CHAR(10) NOT NULL,\
    P_RETAILPRICE FLOAT NOT NULL,\
    P_COMMENT VARCHAR(23) NOT NULL\
);
EOF
)
execute "$create_table_part"

create_table_supplier=$(cat <<EOF
CREATE TABLE supplier(\
    S_SUPPKEY INTEGER NOT NULL,\
    S_NAME CHAR(25) NOT NULL,\
    S_ADDRESS VARCHAR(40) NOT NULL,\
    S_NATIONKEY INTEGER NOT NULL,\
    S_PHONE CHAR(15) NOT NULL,\
    S_ACCTBAL FLOAT NOT NULL,\
    S_COMMENT VARCHAR(101) NOT NULL\
);
EOF
)
execute "$create_table_supplier"

create_table_customer=$(cat <<EOF
CREATE TABLE customer(\
    C_CUSTKEY INTEGER NOT NULL,\
    C_NAME VARCHAR(25) NOT NULL,\
    C_ADDRESS VARCHAR(40) NOT NULL,\
    C_NATIONKEY INTEGER NOT NULL,\
    C_PHONE CHAR(15) NOT NULL,\
    C_ACCTBAL FLOAT NOT NULL,\
    C_MKTSEGMENT CHAR(10) NOT NULL,\
    C_COMMENT VARCHAR(117) NOT NULL\
);
EOF
)

execute "$create_table_customer"

create_table_orders=$(cat <<EOF
CREATE TABLE orders(\
    O_ORDERKEY INTEGER NOT NULL,\
    O_CUSTKEY INTEGER NOT NULL,\
    O_ORDERSTATUS CHAR(1) NOT NULL,\
    O_TOTALPRICE FLOAT NOT NULL,\
    O_ORDERDATE DATETIME NOT NULL,\
    O_ORDERPRIORITY CHAR(15) NOT NULL,\
    O_CLERK CHAR(15) NOT NULL,\
    O_SHIPPRIORITY INTEGER NOT NULL,\
    O_COMMENT VARCHAR(79) NOT NULL\
);
EOF
)
execute "$create_table_orders"

create_table_lineitem=$(cat <<EOF
CREATE TABLE lineitem(\
    L_ORDERKEY INTEGER NOT NULL,\
    L_PARTKEY INTEGER NOT NULL,\
    L_SUPPKEY INTEGER NOT NULL,\
    L_LINENUMBER INTEGER NOT NULL,\
    L_QUANTITY FLOAT NOT NULL,\
    L_EXTENDEDPRICE FLOAT NOT NULL,\
    L_DISCOUNT FLOAT NOT NULL,\
    L_TAX FLOAT NOT NULL,\
    L_RETURNFLAG CHAR(1) NOT NULL,\
    L_LINESTATUS CHAR(1) NOT NULL,\
    L_SHIPDATE DATETIME NOT NULL,\
    L_COMMITDATE DATETIME NOT NULL,\
    L_RECEIPTDATE DATETIME NOT NULL,\
    L_SHIPINSTRUCT CHAR(25) NOT NULL,\
    L_SHIPMODE CHAR(10) NOT NULL,\
    L_COMMENT VARCHAR(44) NOT NULL\
);
EOF
)
execute "$create_table_lineitem"
}


load_data(){
# 导入数据
print_green "导入数据..."

execute "load $DATA_SUPPLIER_PATH into supplier;"

execute "load $DATA_CUSTOMER_PATH into customer;"

# execute "load $DATA_LINEITEM_PATH into lineitem;"

execute "load $DATA_NATION_PATH into nation;"

# execute "load $DATA_ORDERS_PATH into orders;"

# execute "load $DATA_PART_PATH into part;"

# execute "load $DATA_PARTSUPP_PATH into partsupp;"

# execute "load $DATA_REGION_PATH into region;"

print_green "导入数据完成"
}

create_index(){
print_green "创建索引..."

execute "create index supplier(S_SUPPKEY);"
# execute "create index nation(N_NATIONKEY);"
# execute "create index customer(C_CUSTKEY);"

print_green "创建索引完成"
}

print_green "====================================================="
print_green "                    Query Parse Test Start"
print_green "====================================================="
print_white "ROOT_PATH: $ROOT_PATH"
# 检查db目录是否存在，是的话将其删掉
if [ -d "$DB_PATH" ]; then
    rm -rf $DB_PATH;
fi

# 首先启动server
print_green "启动server"
$SERVER_PATH -d $DB_PATH -p $SERVE_PORT > $SERVER_LOG_PATH 2>&1  &

sleep 3;
print_green "启动server成功"

print_green "------------- 小数据集 --------------"
print_green "=> Create Table";

# 创建表
create_table_student=$(cat <<EOF
CREATE TABLE student(\
    s_id INTEGER NOT NULL,\
    s_name VARCHAR(25) NOT NULL,\
    s_age INTEGER NOT NULL,\
    s_birthday DATETIME\
);
EOF
)
execute "$create_table_student"

create_table_teacher=$(cat <<EOF
CREATE TABLE teacher(\
    t_id INTEGER NOT NULL,\
    t_name VARCHAR(25) NOT NULL,\
    t_age INTEGER NOT NULL,\
    t_birthday DATETIME\
);
EOF
)
execute "$create_table_teacher"

create_table_course=$(cat <<EOF
CREATE TABLE course(\
    c_id INTEGER NOT NULL,\
    c_name VARCHAR(25) NOT NULL,\
    c_teacher INTEGER\
);
EOF
)
execute "$create_table_course"

create_table_student_course=$(cat <<EOF
CREATE TABLE sc(\
    sc_sid INTEGER NOT NULL,\
    sc_cid INTEGER NOT NULL\
);
EOF
)
execute "$create_table_student_course"

# execute "show tables;"

print_green "=> Insert"

execute "INSERT INTO student values(5, 'aaa', 21, '1996-01-23');"
execute "INSERT INTO student values(1, 'bbb', 25, '2001-02-03');"
execute "INSERT INTO student values(4, 'ccc', 30, '2002-01-23');"
execute "INSERT INTO student values(2, 'ddd', 22, '1994-08-12');"
execute "INSERT INTO student values(3, 'eee', 35, '2000-01-23');"
execute "INSERT INTO student values(3, 'eee', 35, '2000-01-23');"
execute "INSERT INTO student values(6, 'fff', 35, '1999-01-23');"


execute "INSERT INTO teacher values(1, 'aaa', 35, '1999-01-23');"
execute "INSERT INTO teacher values(2, 'ddd', 49, '1979-01-23');"
execute "INSERT INTO teacher values(3, 'bbb', 50, '1959-01-23');"
execute "INSERT INTO teacher values(4, 'ccc', 33, '1993-01-23');"

execute "INSERT INTO course values(1, 'math', 1);"
execute "INSERT INTO course values(2, 'english', 2);"
execute "INSERT INTO course values(3, 'dbms', 3);"

execute "INSERT INTO sc values(1, 1);"
execute "INSERT INTO sc values(2, 1);"
execute "INSERT INTO sc values(3, 1);"
execute "INSERT INTO sc values(4, 1);"
execute "INSERT INTO sc values(5, 1);"
execute "INSERT INTO sc values(1, 2);"
execute "INSERT INTO sc values(3, 2);"
execute "INSERT INTO sc values(5, 2);"
execute "INSERT INTO sc values(2, 3);"
execute "INSERT INTO sc values(4, 3);"
execute "INSERT INTO sc values(5, 3);"
execute "INSERT INTO sc values(6, 3);"


print_green "-------- NestLoop Test --------"

print_green "=> 设置为NestLoop Join"
execute "SET enable_nestloop = true;"
execute "SET enable_sortmerge = false;"
execute "SET enable_hashjoin = false;"
execute "SELECT s_name, c_name FROM student, course, sc where s_id = sc_sid AND sc_cid = c_id;"


print_green "-------- SortMerge Test --------"

print_green "=> 设置为SortMerge Join"
execute "SET enable_nestloop = false;"
execute "SET enable_sortmerge = true;"
execute "SET enable_hashjoin = false;";

execute "SELECT s_name, c_name FROM student, course, sc where s_id = sc_sid AND sc_cid = c_id;"

# print_green "=> Select"

# execute "SELECT * FROM student;"

# print_green "==> order"
# execute "SELECT * FROM student order by s_age;"

# print_green "==> unique"
# execute "SELECT UNIQUE * FROM student;"

# print_green "==> condition"
# execute "SELECT * FROM student where s_age = 35;"

# print_green "===> condition order"
# execute "SELECT * FROM student where s_age = 35 order by s_birthday;"

# print_green "===> condition unique"
# execute "SELECT UNIQUE * FROM student where s_age = 35;"

# print_green "===> nested"
# execute "SELECT * FROM student where s_age in (SELECT t_age from teacher);"

# print_green "===> group"
# execute "SELECT s_age, COUNT(*) FROM student group by s_age;"



# print_green "=> Drop Table"
# execute "show tables;"
# execute "drop table student;"
# execute "drop table teacher;"
# execute "show tables;"

# print_green "------------- 大数据集 --------------"

# create_tables;

# create_index;

# load_data;

# execute "SELECT UNIQUE S_NATIONKEY FROM supplier; "

# execute "SELECT UNIQUE S_NATIONKEY FROM supplier order by S_NATIONKEY; "


print_green "====================================================="
print_green "                    Query Parse Test End"
print_green "====================================================="