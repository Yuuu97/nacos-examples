package com.alibaba.nacos.example.remote;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

/**
 * Feign 声明式服务调用客户端。
 * {@code name} 对应 Provider 的 {@code spring.application.name}，作为 Nacos 服务发现的 serviceId。
 * Feign 通过 Nacos DiscoveryClient 获取 Provider 实例列表，配合 LoadBalancer 选择实例后发起 HTTP 调用。
 */
@FeignClient(name = "nacos-openfeign-provider-example", path = "/user")
public interface UserFeignClient {

    @GetMapping("/hello")
    String hello();

    @GetMapping("/info")
    String getUserInfo();

    @GetMapping("/detail/{id}")
    String getUserById(@PathVariable("id") Long id);

    @GetMapping("/query")
    String queryUser(@RequestParam("name") String name);

    /** 获取 Provider 实例信息，用于验证负载均衡效果。 */
    @GetMapping("/instance")
    Map<String, Object> getInstanceInfo();

    /** 模拟延迟调用。 */
    @GetMapping("/slow")
    String slowQuery(@RequestParam(value = "delay", defaultValue = "200") int delayMs);
}
