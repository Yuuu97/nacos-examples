package com.alibaba.nacos.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

/**
 * OpenFeign 服务消费者启动类。
 * {@code @EnableFeignClients} 扫描 {@code @FeignClient} 接口生成动态代理，
 * {@code @LoadBalanced} 为 RestTemplate 注入负载均衡拦截器，
 * Nacos Discovery 自动从注册中心拉取 Provider 实例列表。
  * @author qinyu
 */
@EnableDiscoveryClient
@EnableFeignClients(basePackages = "com.alibaba.nacos.example.remote")
@SpringBootApplication
public class NacosConsumerExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(NacosConsumerExampleApplication.class, args);
    }

    /**
     * 注册带有 LoadBalancer 拦截器的 RestTemplate，支持通过服务名代替 IP:PORT 进行调用。
     */
    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

}
