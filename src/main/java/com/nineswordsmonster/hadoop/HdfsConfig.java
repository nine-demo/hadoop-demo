package com.nineswordsmonster.hadoop;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Create by 王佳
 *
 * @author 王佳
 * @date 2020/1/7 15:42
 */
@Data
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "hdfs")
public class HdfsConfig {
    private String path;
    private String username;
}
