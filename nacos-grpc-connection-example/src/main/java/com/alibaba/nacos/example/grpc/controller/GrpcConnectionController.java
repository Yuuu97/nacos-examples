package com.alibaba.nacos.example.grpc.controller;

import com.alibaba.nacos.example.grpc.demo.GrpcConnectionLifecycleDemo;
import com.alibaba.nacos.example.grpc.demo.RpcRequestDispatchDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * gRPC 连接内核演示 Controller
 *
 * @author qinyu
 */
@RestController
@RequestMapping("/grpc")
public class GrpcConnectionController {

    @Autowired
    private GrpcConnectionLifecycleDemo connectionLifecycleDemo;

    @Autowired
    private RpcRequestDispatchDemo rpcRequestDispatchDemo;

    /**
     * 查看 gRPC 连接状态
     */
    @GetMapping("/connection/status")
    public Map<String, Object> connectionStatus() {
        return connectionLifecycleDemo.getConnectionStatus();
    }

    /**
     * 连接详细信息
     */
    @GetMapping("/connection/info")
    public Map<String, Object> connectionInfo() {
        return connectionLifecycleDemo.getConnectionInfo();
    }

    /**
     * 手动触发健康检查
     */
    @PostMapping("/connection/check")
    public Map<String, Object> healthCheck() {
        return connectionLifecycleDemo.simulateHeartBeat();
    }

    /**
     * RPC 请求统计
     */
    @GetMapping("/rpc/stats")
    public Map<String, Object> rpcStats() {
        return rpcRequestDispatchDemo.getRpcStats();
    }

    /**
     * 模拟同步 RPC 调用
     */
    @PostMapping("/rpc/sync")
    public Map<String, Object> syncCall(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName) {
        return rpcRequestDispatchDemo.simulateSyncCall(serviceName, groupName);
    }

    /**
     * 模拟异步 RPC 调用
     */
    @PostMapping("/rpc/async")
    public Map<String, Object> asyncCall(
            @RequestParam(defaultValue = "example-service") String serviceName,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String groupName) {
        try {
            CompletableFuture<Map<String, Object>> future =
                    rpcRequestDispatchDemo.simulateAsyncCall(serviceName, groupName);
            return future.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("success", false);
            error.put("error", e.getMessage());
            return error;
        }
    }

    /**
     * 查看连接事件日志
     */
    @GetMapping("/event-log")
    public Map<String, Object> eventLog() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("events", connectionLifecycleDemo.getEventLog());
        return result;
    }
}
