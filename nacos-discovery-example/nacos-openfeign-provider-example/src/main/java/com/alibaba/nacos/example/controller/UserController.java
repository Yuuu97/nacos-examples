package com.alibaba.nacos.example.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 用户服务 REST 控制器。
 * 启动时通过 Nacos Discovery 自动注册到 Nacos，并通过 BeatReactor 每 5 秒发送心跳保活。
 */
@RestController
@RequestMapping("/user")
public class UserController {

    private static final Logger log = LoggerFactory.getLogger(UserController.class);
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Value("${server.port:8081}")
    private int serverPort;

    @Value("${spring.application.name:nacos-openfeign-provider-example}")
    private String applicationName;

    @GetMapping("/hello")
    public String hello() {
        log.info("==> 收到请求: GET /user/hello");
        return "Hello, 我是用户服务！";
    }

    @GetMapping("/info")
    public String getUserInfo() {
        log.info("==> 收到请求: GET /user/info");
        return "用户信息：张三，VIP会员";
    }

    @GetMapping("/detail/{id}")
    public String getUserById(@PathVariable("id") Long id) {
        log.info("==> 收到请求: GET /user/detail/{}", id);
        return "用户ID: " + id + "，姓名：张三，邮箱：zhangsan@example.com";
    }

    @GetMapping("/query")
    public String queryUser(@RequestParam("name") String name) {
        log.info("==> 收到请求: GET /user/query?name={}", name);
        return "查询用户：" + name + "，状态：正常";
    }

    /**
     * 返回当前 Provider 实例信息（IP、端口、时间戳），用于验证 Nacos 负载均衡效果。
     * 多个 Provider 实例注册到 Nacos 时，Consumer 每次调用可能路由到不同实例。
     */
    @GetMapping("/instance")
    public Map<String, Object> getInstanceInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("serviceName", applicationName);
        info.put("host", getLocalHost());
        info.put("port", serverPort);
        info.put("timestamp", LocalDateTime.now().format(DT_FMT));
        info.put("status", "UP");
        log.info("==> 返回实例信息: {}:{}", getLocalHost(), serverPort);
        return info;
    }

    /**
     * 模拟延迟响应，用于测试超时重试、熔断降级等场景。
     */
    @GetMapping("/slow")
    public String slowQuery(@RequestParam(value = "delay", defaultValue = "200") int delayMs) {
        log.info("==> 收到请求: GET /user/slow?delay={}ms", delayMs);
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "模拟延迟 " + delayMs + "ms 后的响应，来自 " + applicationName + ":" + serverPort;
    }

    /**
     * 健康检查端点，供 Nacos 探针和消费者手动检测。
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "UP");
        result.put("serviceName", applicationName);
        result.put("host", getLocalHost() + ":" + serverPort);
        result.put("timestamp", LocalDateTime.now().format(DT_FMT));
        result.put("memory", Runtime.getRuntime().totalMemory() / 1024 / 1024 + " MB");
        return result;
    }

    private String getLocalHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("无法获取本机 IP，使用 localhost", e);
            return "127.0.0.1";
        }
    }
}
