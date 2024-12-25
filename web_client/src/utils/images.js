import { getFileObjByFileId } from "@/api/file.js";
import { Storage } from '@/utils/cache'
export function getImgSrcByFileObj(fileObj){
    return `data:${fileObj.mimeType};base64,${fileObj.fileContent}`
}

export const getImages = (imgSrc, fileId) => {
    return getFileObjByFileId(fileId)
        .then((res) => {
            imgSrc.value = getImgSrcByFileObj(res.data)
        })
}

export function getImageBase64WithCache(imgSrcList, idx, fileId){
    let fileObj = Storage.getItem(fileId);
    if(fileObj !== null){
        imgSrcList.value[idx] = getImgSrcByFileObj(fileObj);
        console.log("using cache")
        console.log(fileObj)
        console.log(imgSrcList.value[idx])
        return;
    }
    getFileObjByFileId(fileId).then((res) => {
        let fileObj = res.data;
        Storage.setItem(fileObj.fileId, fileObj);
        console.log("request server")
        console.log(fileObj)
        console.log(imgSrcList.value[idx])
        imgSrcList.value[idx] = getImgSrcByFileObj(fileObj);
    })
}