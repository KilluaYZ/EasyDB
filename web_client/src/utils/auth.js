import { Storage } from '@/utils/cache'

function logout(){
  Storage.removeItem('userInfo')
  // Storage.removeItem('token')
  removeUserInfo();
  removeToken();
  location.reload();
}

function setUserInfo(userInfo){
  Storage.setItem('userInfo', userInfo);
}

function getUserInfo(){
  let userInfo = Storage.getItem('userInfo');
  return userInfo;
}

function removeUserInfo(){
  Storage.removeItem('userInfo');
}

function getToken(){
  let token = Storage.getItem("token");
  return token;
}

function setToken(token){
  Storage.setItem('token', token);
}

function removeToken(){
  Storage.removeItem('token');
}

export {
  logout, getToken, setToken, removeToken, setUserInfo, getUserInfo, removeUserInfo
}
