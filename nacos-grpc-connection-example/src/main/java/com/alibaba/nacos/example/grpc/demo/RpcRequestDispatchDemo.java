package com.alibaba.nacos.example.grpc.demo;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RPC 请求调度演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (十一)：RPC 请求调度 —— 收发模型与线程池处理》
 *
 * 演示 Nacos gRPC 通信的三种请求模式以及服务端双线程池架构：
 *
 * 1. 同步请求（RpcClient.request()）
 *    - 阻塞等待响应
 *    - 重试机制（默认 3 次）
 *    - UN_REGISTER 错误 → 异步切换服务器
 *
 * 2. 异步请求（RpcClient.asyncRequest()）
 *    - Futures.addCallback() + Futures.withTimeout()
 *    - 非阻塞，回调处理结果
 *
 * 3. Future 模式（GrpcConnection.requestFuture()）
 *    - 返回 RequestFuture（暴露 get()/isDone()/get(timeout)）
 *
 * 服务端双线程池架构：
 *   GrpcSdkServer.getRpcExecutor() → GlobalExecutor.sdkRpcExecutor
 *   GrpcClusterServer.getRpcExecutor() → GlobalExecutor.clusterRpcExecutor
 *   参数: corePoolSize = availableProcessors × 16 (默认 128)
 *         workQueue = LinkedBlockingQueue(16384)
 *
 * @author qinyu
 */
@Component
public class RpcRequestDispatchDemo {

    private static final Logger log = LoggerFactory.getLogger(RpcRequestDispatchDemo.class);

    @Autowired
    private GrpcConnectionLifecycleDemo connectionLifecycleDemo;

    /**
     * RPC 调用统计
     */
    private final AtomicInteger syncCallCount = new AtomicInteger(0);
    private final AtomicInteger asyncCallCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failCount = new AtomicInteger(0);

    private final Map<String, Long> callLatency = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("====== RpcRequestDispatchDemo 初始化 ======");
        log.info("RPC 请求调度演示组件已就绪");
        log.info("支持三种请求模式：同步 / 异步 / Future");
        log.info("==========================================");
    }

    /**
     * 模拟同步 RPC 调用
     *
     * 对应 RpcClient.request() 源码：
     *   public Response request(Request request, long timeoutMills) {
     *       int retryTimes = 0;
     *       while (retryTimes <= rpcClientConfig.retryTimes()) {
     *           response = this.currentConnection.request(request, timeoutMills);
     *           if (response instanceof ErrorResponse
     *               && response.getErrorCode() == UN_REGISTER) {
     *               rpcClientStatus.compareAndSet(RUNNING, UNHEALTHY);
     *               switchServerAsync();
     *           }
     *           lastActiveTimeStamp = System.currentTimeMillis();
     *           return response;
     *       }
     *       // 全部失败 → switchServerAsyncOnRequestFail()
     *   }
     */
    public Map<String, Object> simulateSyncCall(String serviceName, String groupName) {
        long startTime = System.nanoTime();
        int callNo = syncCallCount.incrementAndGet();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("callNo", callNo);
        result.put("mode", "SYNC");
        result.put("serviceName", serviceName);
        result.put("groupName", groupName);

        try {
            boolean success = true;
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

            if (success) {
                successCount.incrementAndGet();
                result.put("success", true);
                result.put("elapsedMs", elapsed);
                callLatency.put("sync_" + callNo, elapsed);
            }

            log.info("[同步 RPC #{}] service={}@@{}, 耗时 {} ms", callNo, groupName, serviceName, elapsed);
            log.info("同步调用流程追踪:");
            log.info("  → GrpcConnection.request(request, timeoutMills)");
            log.info("    → grpcFutureServiceStub.request(grpcRequest)");
            log.info("    → future.get(timeout, TimeUnit.MILLISECONDS)");
            log.info("  → 成功后更新 lastActiveTimeStamp");
            log.info("  → 重试机制：最多 {} 次, UN_REGISTER 切换服务器",
                    System.getProperty("nacos.remote.client.grpc.retry.times", "3"));

        } catch (Exception e) {
            failCount.incrementAndGet();
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * 模拟异步 RPC 调用
     *
     * 对应 RpcClient.asyncRequest() 和 GrpcConnection.asyncRequest():
     *   → grpcFutureServiceStub.request(grpcRequest)
     *   → Futures.addCallback(future, callback, executor)
     *   → Futures.withTimeout(future, timeout, executor)
     */
    public CompletableFuture<Map<String, Object>> simulateAsyncCall(String serviceName, String groupName) {
        CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
        int callNo = asyncCallCount.incrementAndGet();
        long startTime = System.nanoTime();

        // 模拟异步执行
        CompletableFuture.runAsync(() -> {
            try {
                // 模拟网络延迟
                Thread.sleep(50 + ThreadLocalRandom.current().nextInt(100));

                long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                successCount.incrementAndGet();

                Map<String, Object> result = new LinkedHashMap<>();
                result.put("callNo", callNo);
                result.put("mode", "ASYNC");
                result.put("success", true);
                result.put("elapsedMs", elapsed);
                result.put("serviceName", serviceName);
                result.put("groupName", groupName);

                callLatency.put("async_" + callNo, elapsed);

                log.info("[异步 RPC #{}] service={}@@{}, 耗时 {} ms", callNo, groupName, serviceName, elapsed);
                log.info("异步调用流程追踪:");
                log.info("  → GrpcConnection.asyncRequest(request, callback)");
                log.info("    → grpcFutureServiceStub.request(grpcRequest)");
                log.info("    → Futures.addCallback(future, callback, executor)");
                log.info("    → Futures.withTimeout(future, timeout, executor)");

                future.complete(result);
            } catch (Exception e) {
                failCount.incrementAndGet();
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    /**
     * 获取 RPC 统计
     */
    public Map<String, Object> getRpcStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("syncCallCount", syncCallCount.get());
        stats.put("asyncCallCount", asyncCallCount.get());
        stats.put("successCount", successCount.get());
        stats.put("failCount", failCount.get());
        stats.put("totalCalls", syncCallCount.get() + asyncCallCount.get());

        // 平均延迟
        if (!callLatency.isEmpty()) {
            double avg = callLatency.values().stream().mapToLong(Long::longValue).average().orElse(0);
            stats.put("averageLatencyMs", String.format("%.2f", avg));
        }

        // 服务端双线程池架构说明
        stats.put("serverThreadPool", Map.of(
                "sdkRpcExecutor",
                "corePoolSize = availableProcessors × 16 (默认128), workQueue = LinkedBlockingQueue(16384)",
                "clusterRpcExecutor",
                "corePoolSize = availableProcessors × 16 (默认128), workQueue = LinkedBlockingQueue(16384)"
        ));

        return stats;
    }
}
