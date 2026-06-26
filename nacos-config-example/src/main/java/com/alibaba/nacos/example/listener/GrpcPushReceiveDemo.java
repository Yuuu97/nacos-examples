package com.alibaba.nacos.example.listener;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * gRPC 推送接收演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (五)：gRPC 推送链路 —— 配置变更下发与动态刷新》
 *
 * 展示 Nacos 客户端通过 gRPC 双向流接收服务端推送的完整链路：
 *
 * 服务端推送链：
 *   RpcConfigChangeNotifier.configDataChanged()
 *     → rpcPushService.pushWithCallback()
 *       → gRPC BiRequestStream 双向流推送 ConfigChangeNotifyRequest
 *
 * 客户端接收链：
 *   GrpcClient.bindRequestStream().onNext(Payload)
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
 *       → consistentWithServer = false  (标记缓存失效)
 *       → notifyListenConfig()  (唤醒后台主循环)
 *   → executeConfigListen()
 *     → checkListenCache()  (ConfigBatchListenRequest 批量 MD5 校验)
 *       → refreshContentAndCheck()  (拉取最新内容)
 *       → listener.receiveConfigInfo(newContent)  ← 此处触发回调
 *
 * @author qinyu
 */
@Component
public class GrpcPushReceiveDemo {

    private static final Logger log = LoggerFactory.getLogger(GrpcPushReceiveDemo.class);

    @Autowired
    private ConfigService configService;

    /**
     * 统计接收到的推送次数
     */
    private final AtomicInteger pushReceivedCount = new AtomicInteger(0);

    /**
     * 最后接收到的配置内容
     */
    private volatile String lastReceivedContent;

    /**
     * 最后一次推送时间
     */
    private volatile long lastPushTime;

    @PostConstruct
    public void init() {
        log.info("====== GrpcPushReceiveDemo 初始化 ======");
        log.info("组件已就绪，配置变更时将展示完整的 gRPC 推送接收链路日志");
        log.info("========================================");
    }

    /**
     * 注册 gRPC 推送监听器
     */
    public void registerListener(String dataId, String group, boolean initNotify) throws NacosException {
        configService.addListener(dataId, group, new Listener() {
            @Override
            public Executor getExecutor() {
                // 使用独立线程池执行回调，避免阻塞 Nacos 内部推送线程
                return Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "grpc-push-listener-" + dataId);
                    t.setDaemon(true);
                    return t;
                });
            }

            @Override
            public void receiveConfigInfo(String configInfo) {
                int count = pushReceivedCount.incrementAndGet();
                lastReceivedContent = configInfo;
                lastPushTime = System.currentTimeMillis();

                log.info("================================================");
                log.info("[gRPC 推送接收 #{}] 收到配置变更通知!", count);
                log.info("  dataId      : {}", dataId);
                log.info("  group       : {}", group);
                log.info("  推送时间    : {}", new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                        .format(new java.util.Date(lastPushTime)));
                log.info("  内容长度    : {} 字符", configInfo != null ? configInfo.length() : 0);
                log.info("  内容前 100 字符: {}", configInfo != null && configInfo.length() > 100 ?
                        configInfo.substring(0, 100) + "..." : configInfo);
                log.info("================================================");
                log.info("推送接收链路追踪：");
                log.info("  ① Server RpcConfigChangeNotifier → gRPC BiRequestStream");
                log.info("  ② Client ConfigRpcTransportClient.handleConfigChangeNotifyRequest()");
                log.info("     → consistentWithServer = false");
                log.info("     → notifyListenConfig() 唤醒后台主循环");
                log.info("  ③ executeConfigListen() → checkListenCache()");
                log.info("     → ConfigBatchListenRequest 批量 MD5 校验");
                log.info("  ④ refreshContentAndCheck() 拉取最新内容");
                log.info("  ⑤ Listener.receiveConfigInfo() ← 当前回调");
            }
        });

        log.info("已注册 gRPC 推送监听器: dataId={}, group={}", dataId, group);
        if (initNotify) {
            log.info("  initNotify=true, 首次注册后将立即回调一次");
        }
    }

    /**
     * 获取推送统计信息
     */
    public PushStats getStats() {
        PushStats stats = new PushStats();
        stats.totalPushes = pushReceivedCount.get();
        stats.lastContent = lastReceivedContent;
        stats.lastPushTime = lastPushTime > 0 ?
                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
                        .format(new java.util.Date(lastPushTime)) : "无";
        return stats;
    }

    /**
     * 推送统计
     */
    public static class PushStats {
        public int totalPushes;
        public String lastContent;
        public String lastPushTime;
    }
}
