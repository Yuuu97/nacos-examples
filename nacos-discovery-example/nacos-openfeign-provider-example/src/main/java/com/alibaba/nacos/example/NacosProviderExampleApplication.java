package com.alibaba.nacos.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Nacos 服务提供者启动类。
 * {@code @SpringBootApplication} 触发自动装配，加载 Nacos 服务注册相关自动配置，
 * 在 Web 容器就绪后将本服务实例（IP、端口、权重、元数据）注册到 Nacos Server。
  * @author qinyu
 */
@SpringBootApplication
public class NacosProviderExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(NacosProviderExampleApplication.class, args);
    }

}
