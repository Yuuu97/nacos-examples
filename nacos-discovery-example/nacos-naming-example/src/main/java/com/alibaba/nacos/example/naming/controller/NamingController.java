package com.alibaba.nacos.example.naming.controller;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.example.naming.demo.NamingServiceDemo;
import com.alibaba.nacos.example.naming.demo.ServiceSubscribeDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * NamingService API 演示 Controller
 *
 * 提供 REST 接口演示 Nacos 注册中心核心 API
 *
 * @author qinyu
 */
@RestController
@RequestMapping("/naming")
public class NamingController {

    @Autowired
    private NamingServiceDemo namingServiceDemo;

    @Autowired
    private ServiceSubscribeDemo serviceSubscribeDemo;

    /**
     * 注册服务实例
     *
     * POST /naming/register?serviceName=example-service&port=8080
     */
    @PostMapping("/register")
    public Map<String, Object> register(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName,
            @RequestParam(defaultValue = "127.0.0.1") String ip,
            @RequestParam(defaultValue = "8080") int port,
            @RequestParam(defaultValue = "DEFAULT") String cluster,
            @RequestParam(defaultValue = "1.0") double weight) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("register.source", "nacos-naming-example");
            metadata.put("register.time", String.valueOf(System.currentTimeMillis()));

            Instance instance = namingServiceDemo.registerInstance(serviceName, groupName, ip, port, cluster, weight, metadata);
            result.put("success", true);
            result.put("instance", Map.of(
                    "ip", instance.getIp(),
                    "port", instance.getPort(),
                    "serviceName", serviceName,
                    "groupName", groupName,
                    "cluster", cluster,
                    "weight", weight,
                    "ephemeral", instance.isEphemeral()
            ));
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 注销服务实例
     *
     * POST /naming/deregister?serviceName=example-service&port=8080
     */
    @PostMapping("/deregister")
    public Map<String, Object> deregister(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName,
            @RequestParam(defaultValue = "127.0.0.1") String ip,
            @RequestParam(defaultValue = "8080") int port) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            namingServiceDemo.deregisterInstance(serviceName, groupName, ip, port);
            result.put("success", true);
            result.put("message", "实例已注销");
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 查询服务实例
     *
     * GET /naming/instances?serviceName=example-service
     */
    @GetMapping("/instances")
    public Map<String, Object> getInstances(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName,
            @RequestParam(defaultValue = "true") boolean healthyOnly) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            List<Instance> instances = namingServiceDemo.getInstances(serviceName, groupName, healthyOnly);
            result.put("success", true);
            result.put("serviceName", serviceName);
            result.put("groupName", groupName);
            result.put("healthyOnly", healthyOnly);
            result.put("count", instances.size());

            List<Map<String, Object>> instanceList = new ArrayList<>();
            for (Instance inst : instances) {
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("ip", inst.getIp());
                info.put("port", inst.getPort());
                info.put("healthy", inst.isHealthy());
                info.put("weight", inst.getWeight());
                info.put("cluster", inst.getClusterName());
                info.put("ephemeral", inst.isEphemeral());
                info.put("metadata", inst.getMetadata());
                instanceList.add(info);
            }
            result.put("instances", instanceList);
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 订阅服务
     *
     * POST /naming/subscribe?serviceName=example-service
     */
    @PostMapping("/subscribe")
    public Map<String, Object> subscribe(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            String subId = serviceSubscribeDemo.subscribe(serviceName, groupName);
            result.put("success", true);
            result.put("subscriptionId", subId);
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 取消订阅
     *
     * POST /naming/unsubscribe?serviceName=example-service
     */
    @PostMapping("/unsubscribe")
    public Map<String, Object> unsubscribe(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            boolean removed = serviceSubscribeDemo.unsubscribe(serviceName, groupName);
            result.put("success", removed);
            result.put("message", removed ? "已取消订阅" : "未找到该订阅");
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * 已订阅服务列表
     */
    @GetMapping("/subscribed-services")
    public Map<String, Object> subscribedServices() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("subscribed", serviceSubscribeDemo.getSubscribedServices());
        return result;
    }

    /**
     * 注册统计
     */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return namingServiceDemo.getStats();
    }

    /**
     * 服务列表
     */
    @GetMapping("/services")
    public Map<String, Object> listServices(
            @RequestParam(defaultValue = "1") int pageNo,
            @RequestParam(defaultValue = "10") int pageSize) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            ListView<String> services = namingServiceDemo.getServicesOfServer(pageNo, pageSize);
            result.put("count", services.getCount());
            result.put("services", services.getData());
        } catch (NacosException e) {
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * Server 状态
     */
    @GetMapping("/server-status")
    public Map<String, Object> serverStatus() {
        try {
            List<Instance> instances = namingServiceDemo.getInstances("example-service", "DEFAULT_GROUP", true);
            return Map.of("status", "UP", "cachedInstances", instances.size());
        } catch (Exception e) {
            return Map.of("status", "DOWN", "error", e.getMessage());
        }
    }
}
