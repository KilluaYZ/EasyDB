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
DOT_ROOT=$SCRIPT_DIR/dots
SQL_DOT_NUM=0

draw_ast(){
    DOT_OUT_PATH=$DOT_ROOT/SQL$SQL_DOT_NUM.dot
    SVG_OUT_PATH=$DOT_ROOT/SQL$SQL_DOT_NUM.png
    SQL_DOT_NUM=$((SQL_DOT_NUM + 1))
    # echo "$1"
    $ROOT_PATH/build/test/parser_ast_printer -s "$1" -o $DOT_OUT_PATH 
    dot -T png $DOT_OUT_PATH -o $SVG_OUT_PATH 
}

execute(){
    print_blue "执行命令: $1"
    echo -e "$1 \n" >> out.sql 
    # if [ "$2" != "." ]; then
    draw_ast "$1";
    # fi
    echo "$1; exit;" | $CLIENT_PATH -p $SERVE_PORT
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


join_test(){

print_green "=> 单条件等值连接"
print_green "==> int上进行单条件等值连接"
execute "SELECT * FROM supplier, nation where S_NATIONKEY = N_NATIONKEY;"

execute "SELECT * FROM supplier, nation where S_SUPPKEY < 10 AND S_NATIONKEY = N_NATIONKEY;"

print_green "==> char上进行单条件等值连接"
execute "SELECT * FROM supplier, customer where S_SUPPKEY < 100 AND C_CUSTKEY < 100 AND S_PHONE = C_PHONE;"

print_green "==> varchar上进行单条件等值连接"
execute "SELECT * FROM supplier, customer where S_SUPPKEY < 100 AND C_CUSTKEY < 100 AND S_NAME = C_NAME;"

print_green "=> 单条件不等值连接"
execute "SELECT * FROM supplier, customer where S_SUPPKEY < 100 AND C_CUSTKEY < 100 AND S_PHONE != C_PHONE;"

print_green "=> 多条件连接"
print_green "==> int, varchar上进行多条件连接"
execute "SELECT * FROM supplier, customer where S_SUPPKEY < 10 AND C_CUSTKEY < 10 AND S_PHONE != C_PHONE AND S_SUPPKEY != C_CUSTKEY;"

print_green "=> 三表连接"
execute "SELECT S_NAME, C_NAME, N_NAME FROM supplier, customer, nation where S_SUPPKEY < 10 AND C_CUSTKEY < 10 AND S_NATIONKEY = N_NATIONKEY AND C_NATIONKEY = N_NATIONKEY;"

print_green "=> 两表卡氏积连接"
execute "SELECT * FROM supplier, customer where S_SUPPKEY < 10 AND C_CUSTKEY < 10;"

}

select_test(){
    print_green "-------- Select Test --------"

    print_green "=> 在float, int, varchar上进行条件选择"

    print_green "==> int上进行条件选择"
    execute "SELECT * FROM supplier where S_SUPPKEY = 10;"
    execute "SELECT * FROM supplier where S_SUPPKEY > 10 AND S_SUPPKEY < 20;"
    execute "SELECT * FROM nation order by N_REGIONKEY;"
    execute "SELECT * FROM nation where N_NATIONKEY > 10 order by N_REGIONKEY;"

    print_green "==> float上进行条件选择"
    execute "SELECT * FROM supplier where S_ACCTBAL < 3000.0;"
    execute "SELECT * FROM supplier where S_ACCTBAL > 1000.5 AND S_SUPPKEY < 2000.1;"

    print_green "==> varchar上进行条件选择"
    execute "SELECT * FROM supplier where S_NAME = 'Supplier#000000015';"
    execute "SELECT * FROM supplier where S_SUPPKEY > 10 AND S_SUPPKEY < 20 AND S_NAME != 'Supplier#000000015';"
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

if [ -d "$DOT_ROOT" ]; then 
    rm -rf $DOT_ROOT;
fi

mkdir -p $DOT_ROOT

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

print_green "=> Select"

# 测试SELCT语句基本用法
execute "SELECT s_id, s_name, s_age, s_birthday FROM student;"

print_green "==> order"
# 测试SELCT语句带order by操作
execute "SELECT s_id, s_name, s_age, s_birthday FROM student order by s_age;"

print_green "==> unique"
# 测试SELCT语句带unique操作
execute "SELECT UNIQUE s_id, s_name, s_age, s_birthday FROM student;"

print_green "==> condition"
# 测试SELCT语句带条件
execute "SELECT s_id, s_name, s_age, s_birthday FROM student where s_age = 35;"

print_green "===> condition order"
# 测试SELCT语句带条件和order by
execute "SELECT s_id, s_name, s_age, s_birthday FROM student where s_age = 35 order by s_birthday;"

print_green "===> condition unique"
# 测试SELCT语句带条件和unique
execute "SELECT UNIQUE s_id, s_name, s_age, s_birthday FROM student where s_age = 35;"

# 测试SELECT语句带join
print_green "===> join"
execute "SELECT s_id, s_name, s_age, s_birthday, c_name FROM student, course, sc WHERE s_id = sc_sid AND c_id = sc_cid;"

# print_green "===> nested"
# execute "SELECT * FROM student where s_age in (SELECT t_age from teacher);"

# print_green "===> group"
# execute "SELECT s_age, COUNT(*) FROM student group by s_age;"

print_green "=> Delete"
execute "select * from student where s_id = 6;"
execute "delete from student where s_id = 6;"
execute "select * from student where s_id = 6;"

print_green "=> Drop Table"
execute "show tables;"
execute "drop table student;"
execute "drop table teacher;"
execute "show tables;"

# print_green "=> syntax error"
# execute "select * from student where s_id = ;"
# execute "insert student values() ;"

print_green "------------- 大数据集 --------------"

create_tables;

load_data;

select_test;

execute "SET enable_nestloop = true;"
execute "SET enable_sortmerge = false;"
execute "SET enable_hashjoin = false;";
join_test;

print_green "====================================================="
print_green "                    Query Parse Test End"
print_green "====================================================="