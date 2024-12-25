<template>

</template>

<script setup lang="ts">
import { useRoute } from "vue-router";
import { onMounted } from "vue";
const route = useRoute();
// console.log(route.query.focus)
const Focus = ref('personal')

const init_data = () => {
    let focus = route.query.focus;
    if (focus != null && focus != undefined) {
        Focus.value = focus;
    }
}

onMounted(init_data)

</script>

<style scoped lang="sass">

</style>