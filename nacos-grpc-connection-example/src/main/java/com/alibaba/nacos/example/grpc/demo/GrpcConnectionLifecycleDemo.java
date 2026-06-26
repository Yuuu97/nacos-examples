package com.alibaba.nacos.example.grpc.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * gRPC 连接生命周期演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (九)：双向流设计 —— 连接创建复用与销毁》
 *
 * 演示 Nacos 客户端 gRPC 连接的完整生命周期，包括：
 * - 连接建立（connectToServer 流程）
 * - 连接复用设计（三层复用：RpcClientFactory → GrpcClient → GrpcConnection）
 * - 连接关闭（四种场景）
 * - 重连机制
 *
 * 连接建立流程（GrpcClient.connectToServer()）：
 *   ① createNewManagedChannel(ip, port)  — 创建 TCP 连接
 *   ② createNewChannelStub(managedChannel) — 创建 Unary futureStub
 *   ③ serverCheck() — 发送 ServerCheckRequest，获取 connectionId
 *   ④ BiRequestStreamGrpc.newStub() — 创建双向流 stub（复用同一 TCP）
 *   ⑤ bindRequestStream() — 绑定双向流回调
 *   ⑥ 构造 GrpcConnection，关联 payloadStreamObserver + futureStub + channel
 *   ⑦ 发送 ConnectionSetupRequest（含版本、标签、能力表）
 *   ⑧ 等待能力协商结果
 *
 * 连接复用三层：
 *   层级1: RpcClientFactory — ConcurrentHashMap<String, RpcClient>
 *   层级2: GrpcClient — currentConnection 永久复用
 *   层级3: GrpcConnection — 同一 ManagedChannel 承载 Unary + 双向流
 *
 * @author qinyu
 */
@Component
public class GrpcConnectionLifecycleDemo {

    private static final Logger log = LoggerFactory.getLogger(GrpcConnectionLifecycleDemo.class);

    @Value("${spring.cloud.nacos.discovery.server-addr:127.0.0.1:8848}")
    private String serverAddr;

    private NamingService namingService;

    /**
     * 连接事件日志
     */
    private final List<ConnectionEvent> eventLog = Collections.synchronizedList(new ArrayList<>());

    /**
     * 心跳计数
     */
    private final AtomicInteger heartBeatCount = new AtomicInteger(0);

    /**
     * 连接状态
     */
    private volatile boolean connected = false;
    private volatile long connectTime;
    private volatile long lastHeartBeatTime;

    @PostConstruct
    public void init() {
        log.info("====== GrpcConnectionLifecycleDemo 初始化 ======");
        log.info("正在连接 Nacos Server: {}", serverAddr);
        try {
            Properties props = new Properties();
            props.setProperty("serverAddr", serverAddr);
            props.setProperty("namespace", "public");

            long startTime = System.currentTimeMillis();

            // 创建 NamingService 会触发 gRPC 连接建立
            // 对应 RpcClient.start() → connectToServer()
            namingService = NamingFactory.createNamingService(props);

            connectTime = System.currentTimeMillis();
            connected = true;
            lastHeartBeatTime = connectTime;

            long elapsed = connectTime - startTime;

            eventLog.add(new ConnectionEvent("CONNECTED", "NamingService 创建成功",
                    String.format("耗时 %d ms, serverAddr=%s", elapsed, serverAddr)));

            log.info("gRPC 连接已建立 (耗时 {} ms)", elapsed);
            log.info("连接建立流程追踪:");
            log.info("  → NamingFactory.createNamingService(props)");
            log.info("    → new NacosNamingService(props)");
            log.info("      → NamingClientProxyDelegate(…)");
            log.info("        → NamingGrpcClientProxy(…)");
            log.info("          → RpcClient.start()");
            log.info("            → connectToServer(serverInfo)  ← 核心建连方法");
            log.info("              ① createNewManagedChannel(ip, port) 创建 TCP");
            log.info("              ② createNewChannelStub(channel) 创建 Unary Stub");
            log.info("              ③ serverCheck() ServerCheckRequest → connectionId");
            log.info("              ④ BiRequestStreamGrpc.newStub() 双向流 Stub");
            log.info("              ⑤ bindRequestStream() 绑定双向流回调");
            log.info("              ⑥ 发送 ConnectionSetupRequest 握手");
            log.info("=========================================");
        } catch (NacosException e) {
            connected = false;
            eventLog.add(new ConnectionEvent("CONNECT_FAILED", "连接失败", e.getMessage()));
            log.error("gRPC 连接建立失败: {}", e.getMessage());
        }
    }

    /**
     * 获取连接状态
     */
    public Map<String, Object> getConnectionStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("connected", connected);
        status.put("connectTime", connectTime > 0 ? new Date(connectTime).toString() : "未连接");
        status.put("lastHeartBeatTime", lastHeartBeatTime > 0 ? new Date(lastHeartBeatTime).toString() : "无");
        status.put("heartBeatCount", heartBeatCount.get());
        status.put("serverAddr", serverAddr);
        status.put("eventLogCount", eventLog.size());
        return status;
    }

    /**
     * 获取连接详细信息（通过反射读取 GrpcClient 内部状态）
     */
    public Map<String, Object> getConnectionInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("connected", connected);
        info.put("serverAddr", serverAddr);

        if (namingService != null) {
            try {
                // 通过反射获取 NamingGrpcClientProxy 中的 RpcClient 状态
                Class<?> namingProxyClass = Class.forName(
                        "com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy");
                // 获取所有 Field 以展示内部结构
                Field[] fields = namingProxyClass.getDeclaredFields();
                for (Field field : fields) {
                    field.setAccessible(true);
                    info.put("proxy_field_" + field.getName(),
                            String.valueOf(field.get(namingService)));
                }
            } catch (Exception e) {
                info.put("reflection_error", "无法获取内部状态: " + e.getMessage());
            }

            // 展示三层复用结构
            info.put("connectionArchitecture", Map.of(
                    "layer1", "RpcClientFactory: ConcurrentHashMap<String, RpcClient> —— 按 clientName 隔离",
                    "layer2", "GrpcClient: currentConnection 永久复用（断线重连前不更换）",
                    "layer3", "GrpcConnection: 同一 ManagedChannel 承载 Unary + 双向流"
            ));
        }
        return info;
    }

    /**
     * 模拟心跳检测
     *
     * 对应 RpcClient 后台线程二的 healthCheck() 流程：
     *   ① reconnectionSignal.poll(connectionKeepAlive, 5000ms) 阻塞等待/超时
     *   ② 超时时检查 lastActiveTimeStamp 是否超过 connectionKeepAlive
     *   ③ 超过 → healthCheck()
     *     → 构造 HealthCheckRequest
     *     → 重试 healthCheckRetryTimes + 1 次（默认 4 次）
     *     → 每次间隔 random.nextInt(500) ms 随机延迟
     *     → currentConnection.request(healthCheckRequest, healthCheckTimeOut)
     *     → 超时 3 秒
     *   ④ 成功 → 更新 lastActiveTimeStamp
     *   ⑤ 失败 → RUNNING → UNHEALTHY → reconnect()
     */
    public Map<String, Object> simulateHeartBeat() {
        Map<String, Object> result = new LinkedHashMap<>();
        int beatNo = heartBeatCount.incrementAndGet();
        lastHeartBeatTime = System.currentTimeMillis();

        result.put("beatNo", beatNo);
        result.put("timestamp", new Date(lastHeartBeatTime).toString());
        result.put("success", connected);

        eventLog.add(new ConnectionEvent("HEARTBEAT", "心跳 #" + beatNo,
                connected ? "成功" : "失败（未连接）"));

        log.info("[心跳 #{}] 健康检查", beatNo);
        log.info("心跳流程追踪（对应 RpcClient 后台线程二）:");
        log.info("  → reconnectionSignal.poll(connectionKeepAlive, 5000ms)");
        log.info("    → 超时，检查 lastActiveTimeStamp");
        log.info("    → healthCheck()");
        log.info("      → HealthCheckRequest（构造探活请求）");
        log.info("      → currentConnection.request(healthCheckRequest, 3000ms)");
        log.info("      → 超时 3 秒，重试 4 次，间隔 random(500ms)");
        log.info("    → 成功 → 更新 lastActiveTimeStamp");
        log.info("    → 连续失败 → RUNNING → UNHEALTHY → reconnect()");

        return result;
    }

    /**
     * 获取事件日志
     */
    public List<ConnectionEvent> getEventLog() {
        return new ArrayList<>(eventLog);
    }

    /**
     * 连接事件
     */
    public static class ConnectionEvent {
        public final String type;
        public final String title;
        public final String detail;
        public final long timestamp;

        ConnectionEvent(String type, String title, String detail) {
            this.type = type;
            this.title = title;
            this.detail = detail;
            this.timestamp = System.currentTimeMillis();
        }
    }

    @PreDestroy
    public void destroy() {
        if (namingService != null) {
            try {
                namingService.shutDown();
                connected = false;
                eventLog.add(new ConnectionEvent("DISCONNECTED", "应用关闭", "NamingService.shutDown()"));
                log.info("gRPC 连接已关闭（应用关闭）");
                log.info("连接关闭场景：应用主动关闭 → RpcClient.shutdown()");
                log.info("  → 状态 SHUTDOWN → shutdownNow() 终止线程 → closeConnection()");
            } catch (NacosException e) {
                log.warn("关闭连接异常", e);
            }
        }
    }
}
