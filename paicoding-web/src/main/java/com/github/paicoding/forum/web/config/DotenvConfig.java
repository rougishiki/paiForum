package com.github.paicoding.forum.web.config;

import io.github.cdimascio.dotenv.Dotenv;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * 环境变量配置类
 * 在应用启动时加载 .env 文件中的环境变量
 * 
 * @author paicoding
 */
@Configuration
@Slf4j
public class DotenvConfig {

    @PostConstruct
    public void init() {
        try {
            // 加载项目根目录下的 .env 文件
            Dotenv dotenv = Dotenv.configure()
                    .directory("./")  // .env 文件在项目根目录
                    .ignoreIfMissing()  // 如果文件不存在则忽略，不报错
                    .load();
            
            // 将 .env 中的变量设置到系统环境变量中
            dotenv.entries().forEach(entry -> {
                System.setProperty(entry.getKey(), entry.getValue());
                log.debug("加载环境变量: {} = {}", entry.getKey(), "****");
            });
            
            log.info(".env 文件加载成功");
        } catch (Exception e) {
            log.warn(".env 文件加载失败（可能不存在），将使用默认配置或系统环境变量", e);
        }
    }
}
