package com.alibaba.nacos.example.controller;

import com.alibaba.nacos.example.remote.UserFeignClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费者 REST 控制器，演示 Feign 声明式调用和 RestTemplate 两种服务调用方式。
 * LoadBalancer 自动从 Nacos 获取健康实例列表，根据权重选择最优实例。
  * @author qinyu
 */
@RestController
@RequestMapping("/api")
public class ConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ConsumerController.class);
    private static final String PROVIDER_SERVICE = "nacos-openfeign-provider-example";

    @Autowired
    private UserFeignClient userFeignClient;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DiscoveryClient discoveryClient;

    // ==================== Feign 声明式调用 ====================

    @GetMapping("/hello")
    public String hello() {
        log.info("==> [Feign] 调用 Provider /user/hello");
        return "Feign 调用结果: " + userFeignClient.hello();
    }

    @GetMapping("/info")
    public String info() {
        log.info("==> [Feign] 调用 Provider /user/info");
        return "Feign 调用结果: " + userFeignClient.getUserInfo();
    }

    @GetMapping("/detail")
    public String detail(@RequestParam(defaultValue = "1001") Long id) {
        log.info("==> [Feign] 调用 Provider /user/detail/{}", id);
        return "Feign 调用结果: " + userFeignClient.getUserById(id);
    }

    @GetMapping("/query")
    public String query(@RequestParam(defaultValue = "张三") String name) {
        log.info("==> [Feign] 调用 Provider /user/query?name={}", name);
        return "Feign 调用结果: " + userFeignClient.queryUser(name);
    }

    // ==================== RestTemplate 调用 ====================

    /**
     * RestTemplate + LoadBalancer 调用，URL 中使用服务名代替 IP:PORT。
     */
    @GetMapping("/rt/hello")
    public String restTemplateHello() {
        log.info("==> [RestTemplate] 调用 Provider /user/hello");
        String result = restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/hello", String.class);
        return "RestTemplate 调用结果: " + result;
    }

    @GetMapping("/rt/instance")
    public Map<String, Object> restTemplateInstance() {
        log.info("==> [RestTemplate] 调用 Provider /user/instance");
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/instance", Map.class);
        return result;
    }

    // ==================== 负载均衡验证 ====================

    /**
     * 同时通过 Feign 和 RestTemplate 获取 Provider 实例信息，对比验证负载均衡。
     */
    @GetMapping("/lb-test")
    public Map<String, Object> loadBalanceTest() {
        log.info("==> 负载均衡对比测试");

        Map<String, Object> feignResult = userFeignClient.getInstanceInfo();

        @SuppressWarnings("unchecked")
        Map<String, Object> rtResult = restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/instance", Map.class);

        Map<String, Object> response = new HashMap<>();
        response.put("feign_target", feignResult);
        response.put("restTemplate_target", rtResult);
        response.put("note", "多次调用观察 instance 的 host:port 变化，验证负载均衡效果");
        return response;
    }

    // ==================== 服务发现信息 ====================

    /**
     * 通过 DiscoveryClient 查询 Nacos 中 "nacos-openfeign-provider-example" 的所有健康实例。
     */
    @GetMapping("/discovery")
    public Map<String, Object> discovery() {
        log.info("==> 查询 Nacos 服务发现信息");

        List<String> services = discoveryClient.getServices();
        List<ServiceInstance> instances = discoveryClient.getInstances(PROVIDER_SERVICE);

        Map<String, Object> result = new HashMap<>();
        result.put("allServices", services);
        result.put("providerServiceName", PROVIDER_SERVICE);
        result.put("instanceCount", instances.size());

        List<Map<String, Object>> instanceList = instances.stream().map(inst -> {
            Map<String, Object> map = new HashMap<>();
            map.put("instanceId", inst.getInstanceId());
            map.put("host", inst.getHost());
            map.put("port", inst.getPort());
            map.put("uri", inst.getUri().toString());
            map.put("metadata", inst.getMetadata());
            map.put("secure", inst.isSecure());
            return map;
        }).toList();
        result.put("instances", instanceList);

        return result;
    }

    // ==================== Feign vs RestTemplate 对比 ====================

    /**
     * 同时通过 Feign 和 RestTemplate 调用 Provider 的多个接口，对比两种方式。
     */
    @GetMapping("/compare")
    public Map<String, Object> compare() {
        log.info("==> Feign vs RestTemplate 综合对比");

        Map<String, Object> feignResults = new HashMap<>();
        feignResults.put("/hello", userFeignClient.hello());
        feignResults.put("/info", userFeignClient.getUserInfo());
        feignResults.put("/detail/1001", userFeignClient.getUserById(1001L));
        feignResults.put("/query?name=张三", userFeignClient.queryUser("张三"));
        feignResults.put("/instance", userFeignClient.getInstanceInfo());

        Map<String, Object> restResults = new HashMap<>();
        restResults.put("/hello", restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/hello", String.class));
        restResults.put("/info", restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/info", String.class));
        restResults.put("/instance", restTemplate.getForObject(
                "http://" + PROVIDER_SERVICE + "/user/instance", Map.class));

        Map<String, Object> result = new HashMap<>();
        result.put("feign", feignResults);
        result.put("restTemplate", restResults);
        return result;
    }
}
