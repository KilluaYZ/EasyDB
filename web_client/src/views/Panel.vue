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
import { ifAxisCrossZero } from "echarts/types/src/coord/axisHelper.js";
NProgress.configure({
    easing: 'ease',
    speed: 500,
    showSpinner: true,
    trickleSpeed: 200,
    minimum: 0.3
})
const route = useRoute();
const SQL_QUERY = ref('');
const TIME_COST = ref('');
const TOTAL_CNT = ref(0);
const QUERY_DISABLE = ref(false);
const wck_client = new WebSocketClient()
var time_start = 0;
var time_end = 0;
var QUERY_LIST: Array<string> = [];
var QUERY_HISTORY_LIST: Array<string> = [];
var CUR_HISTORY_IDX: number = -1;
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
    let tmp_query_list = SQL_QUERY.value.trim().replaceAll(";", ";|").split("|");
    tmp_query_list.forEach(item => {
        if (item.length > 0) {
            // wck_client.send(`${item};`);
            QUERY_LIST.push(item);
            QUERY_HISTORY_LIST.unshift(item);
        }
    })
    SendMsg();
}

const SendMsg = () => {
    if (QUERY_LIST.length > 0) {
        let query = QUERY_LIST.shift();
        wck_client.send(`${query}`);
    }
}

const OnReceiveMsg = (resp) => {
    // console.log(resp)
    time_end = performance.now() as number;
    let data = resp.data.substring(0, resp.data.length - 1);
    let data_json = JSON.parse(data);
    console.log(data_json);
    if (data_json.msg === "success") {
        // if (data_json.data.length !== 0) {
            deploy_table(data_json);
        // }
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
    SendMsg();
    if (QUERY_LIST.length === 0) {
        let duration = (time_end - time_start) / 1000;
        TIME_COST.value = duration.toString().substring(0,5);
        NProgress.done();
        QUERY_DISABLE.value = false;
        CUR_HISTORY_IDX = 0;
    }
}


const TABLE_COLUMNS = ref([])
const TABLE_DATA = ref([])
const deploy_table = (resp) => {
    TABLE_COLUMNS.value = []
    TABLE_DATA.value = []


    let data = resp.data.splice(0, resp.data.length);
    if(data.length === 0){
        data = [[]]
    }
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
}
// onMounted(deploy_table);

const init_func = async () => {
    wck_client.addOnMessageCallBackFunc(OnReceiveMsg);
    try {
        await wck_client.connect();
        console.log("WebSocket connection established");
    } catch (error) {
        console.error("Failed to establish WebSocket connection:", error);
        ElNotification({
            title: "连接失败",
            message: "无法连接到服务器，请检查网络连接",
            type: 'error'
        })
    }
    const query_input_dom = document.getElementsByClassName("query_input")[0];
    query_input_dom.addEventListener('keydown', function (event) {
        // 检查key是否为'Ctrl+Enter'
        if (event.ctrlKey && event.key === 'Enter') {
            // console.log('Enter key pressed');
            // 在这里执行你的函数
            onClickQueryBtn();
        } else if (event.key === 'ArrowUp') {
            updateInput(1);
        } else if (event.key === 'ArrowDown') {
            updateInput(-1)
        }
    });
}

const updateInput = (step: number) => {
    let history_input = '';
    CUR_HISTORY_IDX += step;
    if (CUR_HISTORY_IDX >= QUERY_HISTORY_LIST.length) {
        CUR_HISTORY_IDX = QUERY_HISTORY_LIST.length;
    }

    if (CUR_HISTORY_IDX < 0) {
        CUR_HISTORY_IDX = -1;
    } else {
        history_input = QUERY_HISTORY_LIST[CUR_HISTORY_IDX];
    }

    SQL_QUERY.value = history_input;
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