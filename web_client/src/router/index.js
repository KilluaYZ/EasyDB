import { createRouter, createWebHistory } from 'vue-router'
const routes = [
    {
        path: '/',
        name: 'home',
        component: () => import('@/views/Home.vue')
    },
    {
        path: '/panel',
        name: 'panel',
        component: () => import('@/views/Panel.vue')
    }
]

const router = createRouter({
    history: createWebHistory(),
    routes
})

export default router
