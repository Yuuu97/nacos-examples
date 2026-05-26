package com.alibaba.nacos.example.controller;

import com.alibaba.nacos.example.api.User;
import com.alibaba.nacos.example.api.UserService;
import org.apache.dubbo.config.annotation.DubboReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Dubbo 消费者 REST 控制器，通过 {@code @DubboReference} 注入远程 Dubbo 服务代理发起 RPC 调用。
 * {@code version} 和 {@code group} 必须与 Provider 端的 {@code @DubboService} 保持一致。
 */
@RestController
@RequestMapping("/api")
public class DubboConsumerController {

    private static final Logger log = LoggerFactory.getLogger(DubboConsumerController.class);

    @DubboReference(version = "1.0.0", group = "DEFAULT_GROUP", check = false,
            loadbalance = "roundrobin", retries = 0)
    private UserService userService;

    /**
     * 查询用户 —— 演示基本 Dubbo RPC 调用。
     */
    @GetMapping("/user/{id}")
    public Map<String, Object> getUser(@PathVariable Long id) {
        return doGetUser(id);
    }

    /**
     * 查询用户（别名路径）。
     */
    @GetMapping("/user/get/{id}")
    public Map<String, Object> getUserGet(@PathVariable Long id) {
        return doGetUser(id);
    }

    private Map<String, Object> doGetUser(Long id) {
        log.info("[Dubbo Consumer] 请求用户信息, id={}", id);
        User user = userService.getUser(id);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("user", user);
        result.put("message", "通过 Dubbo RPC 调用 Provider 成功");
        return result;
    }

    /**
     * 获取远程 Provider 实例信息，用于验证负载均衡效果。
     * 多次请求此接口，观察返回的 IP/端口是否轮换。
     */
    @GetMapping("/instance")
    public Map<String, Object> getInstanceInfo() {
        log.info("[Dubbo Consumer] 获取 Provider 实例信息");
        Map<String, Object> remoteInfo = userService.getInstanceInfo();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("providerInstance", remoteInfo);
        result.put("tip", "多次调用观察 providerInstance 的 IP/端口变化，验证负载均衡");
        return result;
    }

    /**
     * 健康检查。
     */
    @GetMapping("/health")
    public Map<String, Object> health() {
        log.info("[Dubbo Consumer] 健康检查");
        String remoteHealth = userService.health();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("consumer", "OK - Dubbo Consumer is running");
        result.put("provider", remoteHealth);
        return result;
    }

    /**
     * 批量调用验证负载均衡。连续调用 N 次 getInstanceInfo()，统计实例分布。
     */
    @GetMapping("/lb-test")
    public Map<String, Object> loadBalanceTest(Integer count) {
        if (count == null || count <= 0) {
            count = 10;
        }
        log.info("[Dubbo Consumer] 负载均衡测试, 调用次数={}", count);

        Map<String, Integer> distribution = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            try {
                Map<String, Object> info = userService.getInstanceInfo();
                String key = info.getOrDefault("ip", "unknown") + ":"
                        + info.getOrDefault("dubboPort", "?");
                distribution.merge(key, 1, Integer::sum);
            } catch (Exception e) {
                distribution.merge("error:" + e.getMessage(), 1, Integer::sum);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("totalCalls", count);
        result.put("distribution", distribution);
        result.put("loadBalanceStrategy", "roundrobin");
        return result;
    }
}
