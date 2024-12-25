<template>

    <el-row style="z-index:100; width: 100%; height: 60px; box-shadow: 0px 2px 12px #9b9b9b; align-items: center;">
        <!--  头部导航栏  -->
        <el-row
            style="display: flex; justify-content: space-between; align-items: center; width: 100%; height: 100%; flex-direction: row; flex-wrap: nowrap; margin: 0px 20px 0px 20px">
            <el-row style="display: flex; width: fit-content; height: 100%; align-items: center" @click="onClickLogo">
                <h1
                    style="height: fit-content; width: fit-content; color: #66b4e0; font-family:'Super Funtime';letter-spacing: 3px;font-size: 40px ">
                    EasyDB</h1>
            </el-row>
        </el-row>
    </el-row>
    <el-row style="width: 100%; height: calc(100% - 60px)">
        <!--    下方内容-->
        <router-view style="height: 100%; width: 100%" />
    </el-row>

    <el-dialog v-model="showLoginDialog" width="400" style="height: fit-content">
        <el-row style="width: 100%; height: fit-content">
            <el-row
                style="width: 100%; height: fit-content; display: flex; justify-content: center;align-items: center; flex-direction: column">
                <span style="font-weight: bold; font-size: xx-large; color: #081642">Pond Memory</span>
                <!--        <span style="font-size: medium; color: #606060">每一种情绪都是真的</span>-->
            </el-row>
            <el-row style="width: 100%; height: fit-content; justify-content: center; align-items: center">
                <transition name='list'>
                    <transition-group name="list">
                        <template v-if='cur_stage === "login"' key='login-form'>
                            <p class="title">欢迎登陆<span class="s-desc">Pond Memory</span></p>
                            <el-form label-position='top' :model='form' :rules='loginRules'>
                                <el-form-item prop='email'>
                                    <el-input v-model='form.email' placeholder='邮箱' class='login-input' size='large' />
                                </el-form-item>
                                <el-form-item prop='password'>
                                    <el-input v-model='form.password' type='password' show-password placeholder='密码'
                                        class='login-input' size='large' />
                                </el-form-item>
                                <el-row style='margin-top: 30px'>
                                    <el-button type='primary' @click="onClickLoginBtn"
                                        class="login-button">确认</el-button>
                                </el-row>
                                <el-row style="display: flex;">
                                    <el-button class='bottom-click-text-btn' text
                                        @click="changeStage('changePwd')">忘记密码？</el-button>
                                    <el-button class='bottom-click-text-btn' text
                                        @click="changeStage('register')">还没有账户？点击注册</el-button>
                                </el-row>
                            </el-form>
                        </template>
                        <template v-else-if='cur_stage === "register"' key='register-form'>
                            <p class="title">欢迎注册<span class="s-desc">Pond Memory</span></p>
                            <el-form label-position='top' :model='form' :rules='registerRules'>
                                <el-form-item prop='userName'>
                                    <el-input v-model='form.userName' placeholder='用户名' size='large' />
                                </el-form-item>
                                <el-form-item prop='email'>
                                    <el-input v-model='form.email' placeholder='邮箱' @change='onRegisterEmailChange'
                                        size='large' />
                                </el-form-item>
                                <el-form-item prop='password'>
                                    <el-input v-model='form.password' type='password' show-password placeholder='密码'
                                        size='large' />
                                </el-form-item>
                                <el-form-item prop='confirmPassword'>
                                    <el-input v-model='form.comfirmPassword' type='password' show-password
                                        placeholder='确认密码' size='large' />
                                </el-form-item>
                                <el-form-item prop='checkCode'>
                                    <div style='display: flex; flex-direction: row; width: 100%'>
                                        <el-input v-model='form.checkCode' placeholder='验证码' size='large' />
                                        <el-button type='success' size='large' @click='onClickGetRegisterCheckCodeBtn'
                                            :disabled='registerPageGetCheckCodeDisabled'>获取验证码
                                            {{ check_code_count_down }}</el-button>
                                    </div>
                                </el-form-item>
                                <el-row style='margin-top: 30px'>
                                    <el-button type='primary' @click="onClickRegisterBtn"
                                        :disabled='registerBtnIsDisabled' class="login-button">确认</el-button>
                                </el-row>
                                <el-row style="display: flex; flex-direction: row-reverse">
                                    <el-button class='bottom-click-text-btn' text
                                        @click="changeStage('login')">已注册账户？点击登录</el-button>
                                </el-row>
                            </el-form>
                        </template>
                        <template v-else-if='cur_stage === "changePwd"' key='change-pwd-form'>
                            <p class="title">修改密码<span class="s-desc">Pond Memory</span></p>
                            <el-form label-position='top' :model='form' :rules='registerRules'>
                                <el-form-item prop='email'>
                                    <el-input v-model='form.email' placeholder='邮箱' @change='onRegisterEmailChange'
                                        size='large' />
                                </el-form-item>
                                <el-form-item prop='password'>
                                    <el-input v-model='form.password' type='password' show-password placeholder='密码'
                                        size='large' />
                                </el-form-item>
                                <el-form-item prop='confirmPassword'>
                                    <el-input v-model='form.comfirmPassword' type='password' show-password
                                        placeholder='确认密码' size='large' />
                                </el-form-item>
                                <el-form-item prop='checkCode'>
                                    <div style='display: flex; flex-direction: row; width: 100%'>
                                        <el-input v-model='form.checkCode' placeholder='验证码' size='large' />
                                        <el-button type='success' size='large' @click='onClickGetChangePwdCheckCodeBtn'
                                            :disabled='changePwdPageGetCheckCodeDisabled'>获取验证码
                                            {{ check_code_count_down }}</el-button>
                                    </div>
                                </el-form-item>
                                <el-row style='margin-top: 30px'>
                                    <el-button type='primary' @click="onClickUpdatePwdBtn"
                                        class="login-button">确认</el-button>
                                </el-row>
                                <el-row style="display: flex;">
                                    <el-button class='bottom-click-text-btn' text
                                        @click="changeStage('login')">已有账户？点击登录</el-button>
                                </el-row>
                            </el-form>
                        </template>
                    </transition-group>
                </transition>
            </el-row>
        </el-row>
    </el-dialog>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { login, register, updatePwd, getRegisterSessionKeyCheckCode, getChangePwdSessionKeyCheckCode, checkIfEmailIsRegisted, userProfile } from '@/api/user'
import { getUserInfo, removeToken, removeUserInfo, setToken, setUserInfo } from "@/utils/auth.js";
import { ArrowDown, User, Setting, SwitchButton } from '@element-plus/icons-vue'
import { useRouter } from "vue-router";
import { getImageBase64WithCache } from '@/utils/images'
const router = useRouter()
const showLoginDialog = ref(false);
const loginDialogStatus = ref('login')
const loginDialogInfoMap = ref({
    login: {
        title: "登录"
    },
    register: {
        title: "注册"
    },
    changePwd: {
        title: "找回密码"
    }
})

type UserInfoType = {
    userId: string,
    userName: string,
    email: string,
    avatar: string
}

const UserInfo = ref<UserInfoType>(getUserInfo())
const count = ref(1)
const enterPressed = ref(false)

type FormType = {
    userName: string,
    password: string,
    email: string,
    comfirmPassword: string,
    checkCode: string
}
const form = ref<FormType>({
    userName: "",
    password: "",
    email: "",
    comfirmPassword: "",
    checkCode: ""
})
const cur_stage = ref('login')
const check_code_count_down = ref<string>('')
const registerBtnIsDisabled = ref(false)
const registerPageGetCheckCodeDisabled = ref(false)
const changePwdPageGetCheckCodeDisabled = ref(false)
const sessionKey = ref('')

const loginRules = ref({
    email: [
        { required: true, message: "邮箱不能为空", trigger: 'blur' },
        // { pattern:  /^([a-zA-Z0-9]+[-_\.]?)+@[a-zA-Z0-9]+\.[a-z]+$/, message: "邮箱格式不正确", trigger: 'blur'}
    ],
    password: [
        { required: true, message: "密码不能为空", trigger: 'blur' },
    ]
})

const registerRules = ref({
    email: [
        { required: true, message: "邮箱不能为空", trigger: 'blur' },
        // { pattern:  /^([a-zA-Z0-9]+[-_\.]?)+@[a-zA-Z0-9]+\.[a-z]+$/, message: "邮箱格式不正确", trigger: 'blur'}
    ],
    userName: [
        { required: true, message: "用户名不能为空", trigger: 'blur' },
    ],
    password: [
        { required: true, message: "密码不能为空", trigger: 'blur' },
        {
            validator: (rule: any, value: string, callback: Function) => {
                if (value !== form.value.comfirmPassword) {
                    callback(new Error("两次输入的密码不一致"));
                } else {
                    callback();
                }
            }, trigger: 'blur'
        }
    ],
    comfirmPassword: [
        { required: true, message: "确认密码不能为空", trigger: 'blur' }
    ],
    checkCode: [
        { required: true, message: "验证码不能为空", trigger: 'blur' },
    ]
})
//
// onMounted(() => {
//     window.addEventListener('keydown', handleKeyPress)
// })
//
// onBeforeUnmount(() => {
//     window.removeEventListener('keydown', handleKeyPress)
// })

function changeStage(stage: string) {
    cur_stage.value = stage
}

function onClickLoginBtn() {
    let email: string = form.value.email;
    let password: string = form.value.password;

    if (email === undefined || password === undefined || email.length === 0 || password.length === 0) {
        ElMessage({
            type: 'error',
            message: '请填完信息'
        })
        return;
    }

    login(email, password).then((res) => {
        console.log(res)
        let token = res.data.token;
        setToken(token);
        userProfile().then(res => {
            let userInfo = res.data.userInfo;
            // let userName = userInfo.userName;
            // let params = {
            //   username: userName,
            //   level: 'admin',
            //   avatar: 'https://i.gtimg.cn/club/item/face/img/2/16022_100.gif'
            // }
            // Storage.setItem('userinfo', params)
            setUserInfo(userInfo);
            // location.reload()
            router.push({
                path: '/'
            }).then(() => {
                location.reload()
            })
            showLoginDialog.value = false;
        })
    }).catch((res) => {

    })
}

function onClickRegisterBtn() {
    let userName: string = form.value.userName;
    let email: string = form.value.email;
    let password: string = form.value.password;
    let confirmPassword: string = form.value.comfirmPassword;
    let checkCode: string = form.value.checkCode;
    if (userName === undefined || email === undefined
        || password === undefined || confirmPassword === undefined || checkCode === undefined
        || userName.length === 0 || email.length === 0
        || password.length === 0 || confirmPassword.length === 0 || checkCode.length === 0) {
        ElMessage({
            type: 'error',
            message: '请填写完整信息'
        })
        return;
    }

    if (password !== confirmPassword) {
        ElMessage({
            type: 'error',
            message: '两次输入的密码不一致，请检查'
        })
        return;
    }

    register(userName, email, password, checkCode, sessionKey.value).then((res) => {
        ElMessage({
            type: 'success',
            message: '注册成功，正在跳转主页面'
        })
        onClickLoginBtn()
    })

}

function onClickUpdatePwdBtn() {
    let email: string = form.value.email;
    let password: string = form.value.password;
    let confirmPassword: string = form.value.comfirmPassword;
    let checkCode: string = form.value.checkCode;
    if (email === undefined
        || password === undefined || confirmPassword === undefined || checkCode === undefined
        || email.length === 0 || password.length === 0 || confirmPassword.length === 0 || checkCode.length === 0) {
        ElMessage({
            type: 'error',
            message: '请填写完整信息'
        })
        return;
    }

    if (password !== confirmPassword) {
        ElMessage({
            type: 'error',
            message: '两次输入的密码不一致，请检查'
        })
        return;
    }

    // register(userName, email, password, checkCode, sessionKey.value).then((res) => {
    //     ElMessage({
    //         type: 'success',
    //         message: '注册成功，正在跳转主页面'
    //     })
    //     onClickLoginBtn()
    // })
    updatePwd(email, password, checkCode, sessionKey.value).then((res) => {
        ElMessage({
            type: 'success',
            message: '密码修改成功,正在跳转主页面'
        })
        onClickLoginBtn();
    })

}

var count_down_interval: any;


function onClickGetRegisterCheckCodeBtn() {
    let userName: string = form.value.userName;
    let email: string = form.value.email;
    let password: string = form.value.password;
    let confirmPassword: string = form.value.comfirmPassword;
    if (userName === undefined || email === undefined
        || password === undefined || confirmPassword === undefined
        || userName.length === 0 || email.length === 0
        || password.length === 0 || confirmPassword.length === 0) {
        ElMessage({
            type: 'error',
            message: '请填写完整信息后再获取验证码'
        })
        return;
    }

    getRegisterSessionKeyCheckCode(userName, email).then((res) => {
        ElMessage({
            type: 'success',
            message: '验证码已发送到邮箱，请及时查收'
        })
        sessionKey.value = res.data.sessionKey;
        registerPageGetCheckCodeDisabled.value = true;
        count_down_interval = setInterval(countDown, 1000)
    }).catch((res) => {

    })
}
function onClickGetChangePwdCheckCodeBtn() {
    let email: string = form.value.email;
    let password: string = form.value.password;
    let confirmPassword: string = form.value.comfirmPassword;
    if (email === undefined || password === undefined || confirmPassword === undefined
        || email.length === 0 || password.length === 0 || confirmPassword.length === 0) {
        ElMessage({
            type: 'error',
            message: '请填写完整信息后再获取验证码'
        })
        return;
    }

    if (password !== confirmPassword) {
        ElMessage({
            type: 'error',
            message: '两次输入密码不一致'
        })
        return;
    }

    getChangePwdSessionKeyCheckCode(email).then((res) => {
        ElMessage({
            type: 'success',
            message: '验证码已发送到邮箱，请及时查收'
        })
        sessionKey.value = res.data.sessionKey;
        changePwdPageGetCheckCodeDisabled.value = true;
        count_down_interval = setInterval(countDown, 1000)
    }).catch((res) => {

    })
}

function countDown() {
    if (check_code_count_down.value === undefined || check_code_count_down.value.length === 0) {
        check_code_count_down.value = "60"
    } else {
        if (parseInt(check_code_count_down.value) - 1 >= 0) {
            check_code_count_down.value = (parseInt(check_code_count_down.value) - 1).toString();
        } else {
            check_code_count_down.value = ''
            clearInterval(count_down_interval);
            registerPageGetCheckCodeDisabled.value = false;
            changePwdPageGetCheckCodeDisabled.value = false;
        }
    }
}

function onRegisterEmailChange() {
    let email: string = form.value.email;
    checkIfEmailIsRegisted(email).then((res) => {
        let isExist = res.data.isExist;
        if (isExist === true) {
            ElMessage({
                type: 'error',
                message: '该邮箱已被注册'
            })
            registerBtnIsDisabled.value = true;
        } else {
            registerBtnIsDisabled.value = false;
        }
    })
}

const onCLickLogoutBtn = () => {
    removeToken();
    removeUserInfo();
    UserInfo.value = null;
    // location.reload()
    router.push({
        path: '/'
    }).then(() => {
        location.reload()
    })
}

const onClickGoToPersonalBtn = () => {
    router.push({
        path: "/personal",
    })
}

const onClickGoToSettingBtn = () => {
    router.push({
        path: "/setting",
    })
}

const onClickLogo = () => {
    router.push({
        path: "/"
    }).then(() => {
        location.reload()
    })
}

const bigAvatarSrc = ref(Array(5))

const init_avatar = () => {
    if (UserInfo.value !== undefined && UserInfo.value !== null) {
        getImageBase64WithCache(bigAvatarSrc, 0, UserInfo.value.avatar);
    }
}

onMounted(init_avatar)

</script>

<style scoped lang="scss">
.container {
    width: 100vw;
    height: fit-content;
    min-height: 100vh;
    overflow: hidden;
    @include flex(center, center);
    height: 100vh;
    position: relative;
    background: linear-gradient(135deg, #f2e9e4, #eadfe8, #f2e9e4, #eadfe8);
    transition: all 0.5s;

    .login-card {
        background-color: #fff;
        max-width: 900px;
        border-radius: 60px 5px 60px 5px;
        display: flex;
        overflow: hidden;

        .left {
            padding: 20px;
            width: 50%;
            background-color: #fff;

            img {
                width: 100%;
                height: 100%;
                object-fit: contain;
            }
        }

        .right {
            padding: 60px 40px;
            background-color: #eff7ff;
            width: 50%;

        }
    }

    .copyright {
        position: absolute;
        left: 50%;
        bottom: 20px;
        transform: translate(-50%, -50%);
        color: #333;
        font-size: 14px;
    }
}

.slide-fade-enter-active {
    transition: all 0.3s ease-out;
}

.slide-fade-leave-active {
    transition: all 0.8s cubic-bezier(1, 0.5, 0.8, 1);
}

.slide-fade-enter-from,
.slide-fade-leave-to {
    transform: translateX(20px);
    opacity: 0;
}

.list-move,
/* 对移动中的元素应用的过渡 */
.list-enter-active,
.list-leave-active {
    transition: all 0.5s ease-in;
}

.list-enter-from,
.list-leave-to {
    opacity: 0;
    //transform: translateX(5px);
    transition: all 0.5s ease-out;
}

/* 确保将离开的元素从布局流中删除
  以便能够正确地计算移动的动画。 */
.list-leave-active {
    position: absolute;
}

.title {
    font-size: 20px;
    color: #333;
    padding-bottom: 40px;
}

.s-desc {
    font-size: 14px;
    color: #999;
    padding-left: 5px;
}

/* 调整副标题文本颜色 */
.sutitle {
    font-size: 16px;
    color: #6e6775;
    padding-bottom: 10px;
}

.login-input {
    box-sizing: border-box;
    width: 100%;
    padding: 10px;
    border-radius: 5px;
}

.login-input:focus {
    border-color: #007bff;
    /* 更改边框颜色 */
    box-shadow: 0 0 5px rgba(0, 123, 255, 0.5);
    /* 添加阴影效果 */
    outline: none;
    /* 去掉默认的外部轮廓线 */
}

.login-button {
    width: 100%;
    //background-color: #0056b3;
    //color: #fff;
    padding: 12px 24px;
    border: none;
    border-radius: 5px;
    cursor: pointer;
    font-size: 18px;
}

//
//.login-button:hover {
//    background-color: #ff6b6b;
//}

.bottom-click-text-btn:hover {
    color: #337ecc;
}
</style>
