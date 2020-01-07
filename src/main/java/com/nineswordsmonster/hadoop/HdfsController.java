package com.nineswordsmonster.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.Map;

/**
 * Create by 王佳
 *
 * @author 王佳
 * @date 2020/1/7 15:51
 */
@Slf4j
@RestController
@RequestMapping("/hdfs")
public class HdfsController {
    @PostMapping(value = "mkdir")
    @ResponseBody
    public ResponseEntity<?> mkdir(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            log.debug("请求参数为空");
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        // 创建空文件夹
        boolean isOk = HdfsService.mkdir(path);
        if (isOk) {
            log.debug("文件夹创建成功");
            return ResponseEntity.status(HttpStatus.OK).body("Success");
        } else {
            log.debug("文件夹创建失败");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error");
        }
    }

    /**
     * 读取HDFS目录信息
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/readPathInfo")
    public ResponseEntity<?> readPathInfo(@RequestParam("path") String path) throws Exception {
        List<Map<String, Object>> list = HdfsService.readPathInfo(path);
        return ResponseEntity.ok().body(list);
    }

    /**
     * 获取HDFS文件在集群中的位置
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/getFileBlockLocations")
    public ResponseEntity<?> getFileBlockLocations(@RequestParam("path") String path) throws Exception {
        BlockLocation[] blockLocations = HdfsService.getFileBlockLocations(path);
        return ResponseEntity.ok().body(blockLocations);
    }

    /**
     * 创建文件
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/createFile")
    public ResponseEntity<?> createFile(@RequestParam("path") String path,
                                        @RequestParam("file") MultipartFile file) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        } else {
            file.getBytes();
        }
        HdfsService.createFile(path, file);
        return ResponseEntity.ok().body("创建文件成功");
    }

    /**
     * 读取HDFS文件内容
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/readFile")
    public ResponseEntity<?> readFile(@RequestParam("path") String path) throws Exception {
        String targetPath = HdfsService.readFile(path);
        return ResponseEntity.ok().body(targetPath);
    }

    /**
     * 读取HDFS文件转换成Byte类型
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/openFileToBytes")
    public ResponseEntity<?> openFileToBytes(@RequestParam("path") String path) throws Exception {
        byte[] files = HdfsService.openFileToBytes(path);
        return ResponseEntity.ok().body(files);
    }

    /**
     * 读取HDFS文件装换成User对象
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
//    @PostMapping("/openFileToUser")
//    public Result openFileToUser(@RequestParam("path") String path) throws Exception {
//        User user = HdfsService.openFileToObject(path, User.class);
//        return new Result(Result.SUCCESS, "读取HDFS文件装换成User对象", user);
//    }

    /**
     * 读取文件列表
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/listFile")
    public ResponseEntity<?> listFile(@RequestParam("path") String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        List<Map<String, String>> returnList = HdfsService.listFile(path);
        return ResponseEntity.ok().body(returnList);
    }

    /**
     * 重命名文件
     *
     * @param oldName
     * @param newName
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/renameFile")
    public ResponseEntity<?> renameFile(@RequestParam("oldName") String oldName, @RequestParam("newName") String newName)
            throws Exception {
        if (StringUtils.isEmpty(oldName) || StringUtils.isEmpty(newName)) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
        boolean isOk = HdfsService.renameFile(oldName, newName);
        return ResponseEntity.ok().body(isOk);
    }

    /**
     * 删除文件
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/deleteFile")
    public ResponseEntity<?> deleteFile(@RequestParam("path") String path) throws Exception {
        boolean isOk = HdfsService.deleteFile(path);
        return ResponseEntity.ok().body(isOk);
    }

    /**
     * 上传文件
     *
     * @param path
     * @param uploadPath
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/uploadFile")
    public ResponseEntity<?> uploadFile(@RequestParam("path") String path, @RequestParam("uploadPath") String uploadPath)
            throws Exception {
        HdfsService.uploadFile(path, uploadPath);
        return ResponseEntity.ok().build();
    }

    /**
     * 下载文件
     *
     * @param path
     * @param downloadPath
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/downloadFile")
    public ResponseEntity<?> downloadFile(@RequestParam("path") String path, @RequestParam("downloadPath") String downloadPath)
            throws Exception {
        HdfsService.downloadFile(path, downloadPath);
        return ResponseEntity.ok().build();
    }

    /**
     * HDFS文件复制
     *
     * @param sourcePath
     * @param targetPath
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/copyFile")
    public ResponseEntity<?> copyFile(@RequestParam("sourcePath") String sourcePath, @RequestParam("targetPath") String targetPath)
            throws Exception {
        HdfsService.copyFile(sourcePath, targetPath);
        return ResponseEntity.ok().build();
    }

    /**
     * 查看文件是否已存在
     *
     * @param path
     *
     * @return
     *
     * @throws Exception
     */
    @PostMapping("/existFile")
    public ResponseEntity<?> existFile(@RequestParam("path") String path) throws Exception {
        boolean isExist = HdfsService.existFile(path);
        return ResponseEntity.ok().body(isExist);
    }
}
