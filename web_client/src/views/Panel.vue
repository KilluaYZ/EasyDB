<template>
    <el-row class="container">
        <el-row class="query">
            <el-input v-model="SQL_QUERY" :autosize="{ minRows: 2 }" type="textarea" placeholder="请输入SQL语句"
                class="query_input" :disabled="QUERY_DISABLE"/>
            <el-button :icon="Search" circle type="primary" class="query_btn" @click="onClickQueryBtn" :disabled="QUERY_DISABLE"/>
        </el-row>

        <el-row class="resp">
            <el-row class="stat">
                <span class="stat_text">查询时间(s): {{ TIME_COST }}</span>
                <span class="stat_text">总记录数: {{ TOTAL_CNT }}</span>
            </el-row>
            <el-row class="table">
                <el-table-v2 :columns="TABLE_COLUMNS" :data="TABLE_DATA" :width="1200" :height="400" fixed />
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
    console.log(`用户查询： ${SQL_QUERY.value}`);
   NProgress.start();
  QUERY_DISABLE.value = true;
    const query_promise = new Promise((resolve, reject) => {
        setTimeout(() => {
            let tmp_resp = {
                "data": [
                    ["name", "age", "class", "address"],
                    ["xiaoming", 12, "class 1", "Hsdfhu"],
                    ["alice", 16, "class 1", "Hsdfhu123123"],
                    ["bob", 13, "class 1", "Hs12341dadfhu"],
                    ["ceylan", 16, "class 2", "y12793rpufdsa"],
                    ["danney", 77, "class 3", "jhq78too2eiwhdv"],
                ],
                "msg": ""
            };
            resolve(tmp_resp);
        }, 1000);
    })

    query_promise.then((resp) => {
        console.log(resp);
        deploy_table(resp);
        NProgress.done();
      QUERY_DISABLE.value = false;

    })
}

const TABLE_COLUMNS = ref([])
const TABLE_DATA = ref([])
const deploy_table = (resp) => {
    let data = resp.data.splice(0, resp.data.length);
    let header = data[0];
    data.shift();
    console.log(header);
    console.log(data);

    header.forEach((item) => {
        TABLE_COLUMNS.value.push({
            key: item,
            dataKey: item,
            title: item,
            width: 150
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

    console.log(TABLE_COLUMNS.value)
    console.log(TABLE_DATA.value)
}
// onMounted(deploy_table);



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
            justify-content: start;

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