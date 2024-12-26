<template>
    <el-row class="container">
        <el-row class="query">
            <el-input v-model="SQL_QUERY" :autosize="{ minRows: 2 }" type="textarea" placeholder="请输入SQL语句"
                class="query_input" :disabled="QUERY_DISABLE" />
            <el-button :icon="Search" circle type="primary" class="query_btn" @click="onClickQueryBtn"
                :disabled="QUERY_DISABLE" />
        </el-row>

        <el-row class="resp">
            <el-row class="stat">
                <span class="stat_text">查询时间(s): {{ TIME_COST }}</span>
                <span class="stat_text">总记录数: {{ TOTAL_CNT }}</span>
            </el-row>
            <el-row class="table" style="height:600px">
                <el-auto-resizer>
                    <template #default="{ height, width }">
                        <el-table-v2 :columns="TABLE_COLUMNS" :data="TABLE_DATA" :width="width" :height="height"
                            fixed />
                    </template>
                </el-auto-resizer>
            </el-row>
        </el-row>
    </el-row>
</template>

<script setup lang="ts">
import { useRoute } from "vue-router";
import { onMounted, ref } from "vue";
import { Search } from '@element-plus/icons-vue';
import 'nprogress/nprogress.css'
import NProgress from 'nprogress'
import { WebSocketClient } from "@/utils/websocketclient"
import { ElNotification } from "element-plus"
NProgress.configure({
    easing: 'ease',
    speed: 500,
    showSpinner: true,
    trickleSpeed: 200,
    minimum: 0.3
})
const route = useRoute();
const SQL_QUERY = ref('');
const TIME_COST = ref(0);
const TOTAL_CNT = ref(0);
const QUERY_DISABLE = ref(false);
const wck_client = new WebSocketClient()
var time_start = null;
var time_end = null;
const generateColumns = (length = 10, prefix = 'column-', props?: any) =>
    Array.from({ length }).map((_, columnIndex) => ({
        ...props,
        key: `${prefix}${columnIndex}`,
        dataKey: `${prefix}${columnIndex}`,
        title: `Column ${columnIndex}`,
        width: 150,
    }))

const generateData = (
    columns: ReturnType<typeof generateColumns>,
    length = 200,
    prefix = 'row-'
) =>
    Array.from({ length }).map((_, rowIndex) => {
        return columns.reduce(
            (rowData, column, columnIndex) => {
                rowData[column.dataKey] = `Row ${rowIndex} - Col ${columnIndex}`
                return rowData
            },
            {
                id: `${prefix}${rowIndex}`,
                parentId: null,
            }
        )
    })


const onClickQueryBtn = () => {
    time_start = performance.now();
    console.log(`用户查询： ${SQL_QUERY.value}`);
    if (SQL_QUERY.value === "") {
        ElNotification({
            title: "SQL语句为空",
            message: "请输入SQL语句",
            type: 'warnning'
        })
        return;
    }
    
    if (SQL_QUERY.value.toLowerCase().startsWith("help")) {
        /*let help_info = "Supported SQL syntax:\n"
            + "  command ;\n"
            + "command:\n"
            + "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
            + "  DROP TABLE table_name\n"
            + "  CREATE INDEX table_name (column_name)\n"
            + "  DROP INDEX table_name (column_name)\n"
            + "  INSERT INTO table_name VALUES (value [, value ...])\n"
            + "  DELETE FROM table_name [WHERE where_clause]\n"
            + "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
            + "  SELECT selector FROM table_name [WHERE where_clause]\n"
            + "type:\n"
            + "  {INT | FLOAT | CHAR(n)}\n"
            + "where_clause:\n"
            + "  condition [AND condition ...]\n"
            + "condition:\n"
            + "  column op {column | value}\n"
            + "column:\n"
            + "  [table_name.]column_name\n"
            + "op:\n"
            + "  {= | <> | < | > | <= | >=}\n"
            + "selector:\n"
            + "  {* | column [, column ...]}\n";*/
        let help_info = "EasyDB支持大多数SQL语句，所以SQL怎么用，你也就怎么用！"
        ElNotification({
            title: "帮助信息",
            message: help_info,
            type: 'info'
        })
        return;
    }

    NProgress.start();
    QUERY_DISABLE.value = true;
    // const query_promise = new Promise((resolve, reject) => {
    //     setTimeout(() => {
    //         let tmp_resp = {
    //             "data": [
    //                 ["name", "age", "class", "address"],
    //                 ["xiaoming", 12, "class 1", "Hsdfhu"],
    //                 ["alice", 16, "class 1", "Hsdfhu123123"],
    //                 ["bob", 13, "class 1", "Hs12341dadfhu"],
    //                 ["ceylan", 16, "class 2", "y12793rpufdsa"],
    //                 ["danney", 77, "class 3", "jhq78too2eiwhdv"],
    //             ],
    //             "msg": ""
    //         };
    //         resolve(tmp_resp);
    //     }, 1000);
    // })
    wck_client.send(`${SQL_QUERY.value};`);
    // query_promise.then((resp) => {
    //     console.log(resp);
    //     deploy_table(resp);
    //     NProgress.done();
    //     QUERY_DISABLE.value = false;
    // })
}

const OnReceiveMsg = (resp) => {
    // console.log(resp)
    time_end = performance.now();
    let data = resp.data.substring(0, resp.data.length - 1);
    let data_json = JSON.parse(data);
    console.log(data_json);
    if (data_json.msg === "success") {
        if (data_json.data.length !== 0) {
            deploy_table(data_json);
        }
        TOTAL_CNT.value = data_json.total;
        ElNotification({
            title: "查询成功",
            message: "你的查询已成功执行",
            type: 'success'
        })
    } else {
        ElNotification({
            title: "查询失败",
            message: `错误原因：${data_json.msg}`,
            type: 'error'
        })
    }
    NProgress.done();
    QUERY_DISABLE.value = false;
}


const TABLE_COLUMNS = ref([])
const TABLE_DATA = ref([])
const deploy_table = (resp) => {
    TABLE_COLUMNS.value = []
    TABLE_DATA.value = []
    let data = resp.data.splice(0, resp.data.length);
    let header = data[0];
    data.shift();
    // console.log(header);
    // console.log(data);

    header.forEach((item) => {
        TABLE_COLUMNS.value.push({
            key: item,
            dataKey: item,
            title: item,
            width: 200
        })
    })

    let data_length = data.length;
    for (let i = 0; i < data_length; i++) {
        let row = data[i];
        let row_length = row.length;
        let res = { id: `row-${i}` }
        for (let j = 0; j < row_length; j++) {
            res[header[j]] = row[j];
        }
        TABLE_DATA.value.push(res);
    }

    // console.log(TABLE_COLUMNS.value)
    // console.log(TABLE_DATA.value)
    let duration = (time_end - time_start) / 1000;
    TIME_COST.value = duration.toString().substring(0,5);
}
// onMounted(deploy_table);

const init_func = () => {
    wck_client.addOnMessageCallBackFunc(OnReceiveMsg);
    wck_client.connect();
    const query_input_dom = document.getElementsByClassName("query_input")[0];
    query_input_dom.addEventListener('keydown', function (event) {
        // 检查key是否为'Enter'
        if (event.key === 'Enter') {
            // console.log('Enter key pressed');
            // 在这里执行你的函数
            onClickQueryBtn();
        }
    });
}

onMounted(init_func)

</script>

<style scoped lang="scss">
.container {
    width: 80%;
    height: 100%;
    background: #FFF;
    display: flex;
    justify-content: center;
    align-items: start;
    padding: 30px;

    .query {
        width: 80%;
        display: flex;
        flex-direction: row;
        justify-content: center;

        .query_input {
            width: 80%;
            margin-right: 20px;
            font-size: 24px;
        }

        .query_btn {}
    }

    .resp {
        display: flex;
        justify-content: center;
        width: 80%;
        flex-direction: column;

        .stat {
            justify-content: center;

            .stat_text {
                font-size: 18px;
                padding: 10px 20px 10px 20px;
            }
        }

        .table {
            justify-content: center;
        }
    }

}
</style>