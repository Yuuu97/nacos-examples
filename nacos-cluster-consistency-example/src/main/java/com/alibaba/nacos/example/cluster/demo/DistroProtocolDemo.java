package com.alibaba.nacos.example.cluster.demo;

import com.alibaba.nacos.api.exception.NacosException;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distro 协议数据同步与容错处理演示
 *
 * 对应文章：
 *   (十三)：Distro 协议 ——AP 模式异步数据同步原理
 *   (十四)：Distro 容错处理 —— 数据校验与冲突修复
 *
 * 演示 Distro 协议的核心机制：
 *
 * 1. 数据同步（DistroProtocol.sync()）
 *    - 双层任务引擎：DelayTask 延迟合并窗口 → ExecuteTask 执行发送
 *    - DistroDelayTask.merge() 三条裁决规则
 *    - gRPC 异步回调 vs 同步发送双模式
 *
 * 2. 容错处理（DistroVerifyTimedTask）
 *    - 每 5 秒 revision 校验
 *    - 两段式判等
 *    - 校验失败 → ClientVerifyFailedEvent → 零延迟补偿同步
 *
 * 核心源码：
 *   DistroProtocol.sync(distroKey, action)
 *     → allMembersWithoutSelf() 遍历集群
 *     → DistroDelayTask (1000ms 延迟窗口)
 *     → DistroDelayTaskProcessor → DistroSyncChangeTask/DistroSyncDeleteTask
 *     → DistroTransportAgent.syncData() gRPC 发送
 *
 *   DistroVerifyTimedTask.run()
 *     → 每 5 秒执行
 *     → getVerifyData() → DistroClientVerifyInfo(clientId, revision)
 *     → processVerifyData() → verifyClient() 两段式判等
 *     → 失败 → ClientVerifyFailedEvent → syncToVerifyFailedServer()
 *
 * @author qinyu
 */
@Component
public class DistroProtocolDemo {

    private static final Logger log = LoggerFactory.getLogger(DistroProtocolDemo.class);

    @Autowired
    private ClusterMemberManagerDemo clusterMemberManager;

    /**
     * 本地数据存储（模拟 DistroDataStorage）
     */
    private final ConcurrentHashMap<String, DistroDataEntry> dataStore = new ConcurrentHashMap<>();

    /**
     * 数据同步记录
     */
    private final List<SyncRecord> syncRecords = Collections.synchronizedList(new ArrayList<>());

    /**
     * 数据校验记录
     */
    private final List<VerifyRecord> verifyRecords = Collections.synchronizedList(new ArrayList<>());

    /**
     * 版本号生成器（模拟 revision）
     */
    private final AtomicLong revisionGenerator = new AtomicLong(0);

    /**
     * 执行统计
     */
    private final AtomicInteger syncCount = new AtomicInteger(0);
    private final AtomicInteger verifyCount = new AtomicInteger(0);
    private final AtomicInteger conflictCount = new AtomicInteger(0);

    @PostConstruct
    public void init() {
        log.info("====== DistroProtocolDemo 初始化 ======");
        log.info("Distro 协议演示组件已就绪");
        log.info("========================================");
    }

    /**
     * 模拟 Distro 数据同步
     *
     * 对应 DistroProtocol.sync() 完整流程：
     *   ① distroProtocol.sync(distroKey, DataOperation.CHANGE)
     *   ② allMembersWithoutSelf() 为每个远程节点创建 DistroDelayTask
     *   ③ NacosDelayTaskExecuteEngine.addTask() 延迟合并窗口
     *   ④ DistroDelayTaskProcessor.process() 路由到执行任务
     *   ⑤ AbstractDistroExecuteTask.run() 执行发送
     *   ⑥ 接收端 DistroClientDataProcessor.processData() 增量同步
     */
    public Map<String, Object> simulateSync(String dataKey, String content) {
        int syncNo = syncCount.incrementAndGet();
        long revision = revisionGenerator.incrementAndGet();

        // 1. 写入本地存储
        DistroDataEntry entry = new DistroDataEntry(dataKey, content, revision, System.currentTimeMillis());
        dataStore.put(dataKey, entry);

        // 2. 模拟发送到所有远程节点
        List<Map<String, Object>> targets = new ArrayList<>();
        List<Map<String, Object>> members = clusterMemberManager.getMembers();

        for (Map<String, Object> member : members) {
            String address = (String) member.get("address");
            String state = (String) member.get("state");
            boolean isSelf = false; // 简化处理

            if (!isSelf) {
                boolean success = "UP".equals(state);
                targets.add(Map.of(
                        "target", address,
                        "success", success,
                        "state", state
                ));

                syncRecords.add(new SyncRecord(dataKey, address, revision, success));

                log.info("[Distro 同步 #{}] dataKey={}, target={}, revision={}, success={}",
                        syncNo, dataKey, address, revision, success);
            }
        }

        log.info("[Distro 同步 #{}] 完成", syncNo);
        log.info("同步流程追踪:");
        log.info("  → DistroProtocol.sync(distroKey, DataOperation.CHANGE)");
        log.info("    → 遍历 allMembersWithoutSelf()");
        log.info("    → DistroDelayTask (延迟窗口 1000ms)");
        log.info("      → merge() 三条裁决规则:");
        log.info("        ① 类型安全: 仅同类型合并");
        log.info("        ② 操作裁决: DELETE > CHANGE");
        log.info("        ③ 继承起始计时");
        log.info("    → DistroDelayTaskProcessor.process()");
        log.info("      → DistroSyncChangeTask/DistroSyncDeleteTask");
        log.info("    → DistroTransportAgent.syncData() gRPC 发送");
        log.info("    → 接收端 DistroClientDataProcessor.processData()");
        log.info("      → upgradeClient() 增量同步（全量比对 + 同比 + 清理）");

        return Map.of(
                "syncNo", syncNo,
                "dataKey", dataKey,
                "revision", revision,
                "targets", targets,
                "contentLength", content.length()
        );
    }

    /**
     * 模拟 Distro 数据校验
     *
     * 对应 DistroVerifyTimedTask 完整流程：
     *   ① 每 5 秒执行，遍历 dataStorageTypes
     *   ② getVerifyData() 构造 DistroClientVerifyInfo(clientId, revision)
     *   ③ 发送校验请求
     *   ④ 接收端 processVerifyData() → verifyClient()
     *      → 两段式判等: revision == 0 (向后兼容) || 精确匹配
     *   ⑤ 失败 → ClientVerifyFailedEvent → syncToVerifyFailedServer()
     */
    public Map<String, Object> simulateVerify() {
        int verifyNo = verifyCount.incrementAndGet();
        List<Map<String, Object>> results = new ArrayList<>();

        for (Map.Entry<String, DistroDataEntry> entry : dataStore.entrySet()) {
            String dataKey = entry.getKey();
            DistroDataEntry dataEntry = entry.getValue();

            // 模拟发送校验请求到所有远程节点
            List<Map<String, Object>> members = clusterMemberManager.getMembers();
            for (Map<String, Object> member : members) {
                String address = (String) member.get("address");
                String state = (String) member.get("state");

                // 模拟两段式判等
                boolean matched = "UP".equals(state) ||
                        ("SUSPICIOUS".equals(state) && ThreadLocalRandom.current().nextBoolean());

                if (!matched) {
                    conflictCount.incrementAndGet();
                    // 模拟补偿同步
                    verifyRecords.add(new VerifyRecord(dataKey, address, dataEntry.revision, false, true));
                    log.warn("[Distro 校验 #{}] 数据不一致! dataKey={}, target={}, 执行补偿同步",
                            verifyNo, dataKey, address);
                    log.warn("补偿同步流程:");
                    log.warn("  → processVerifyData() → verifyClient() 两段式判等");
                    log.warn("  → 不一致 → ClientVerifyFailedEvent");
                    log.warn("  → syncToVerifyFailedServer()");
                    log.warn("    → distroProtocol.syncToTarget(key, ADD, targetServer, 0L)");
                    log.warn("    → 零延迟，立即执行");
                } else {
                    verifyRecords.add(new VerifyRecord(dataKey, address, dataEntry.revision, true, false));
                }

                results.add(Map.of(
                        "dataKey", dataKey,
                        "target", address,
                        "localRevision", dataEntry.revision,
                        "matched", matched,
                        "compensated", !matched
                ));
            }
        }

        log.info("[Distro 校验 #{}] 完成: 数据条目={}, 不一致={}",
                verifyNo, dataStore.size(), conflictCount.get());

        return Map.of(
                "verifyNo", verifyNo,
                "totalEntries", dataStore.size(),
                "totalConflicts", conflictCount.get(),
                "details", results
        );
    }

    /**
     * 获取数据同步状态
     */
    public Map<String, Object> getSyncStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("dataStoreSize", dataStore.size());
        status.put("totalSyncs", syncCount.get());
        status.put("totalVerifies", verifyCount.get());
        status.put("totalConflicts", conflictCount.get());

        List<Map<String, Object>> dataEntries = new ArrayList<>();
        dataStore.forEach((key, entry) -> {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("dataKey", key);
            info.put("revision", entry.revision);
            info.put("contentLength", entry.content.length());
            info.put("lastUpdateTime", new Date(entry.lastUpdateTime).toString());
            dataEntries.add(info);
        });
        status.put("dataEntries", dataEntries);

        return status;
    }

    /**
     * Distro 数据条目
     */
    static class DistroDataEntry {
        final String dataKey;
        final String content;
        final long revision;
        final long lastUpdateTime;

        DistroDataEntry(String dataKey, String content, long revision, long lastUpdateTime) {
            this.dataKey = dataKey;
            this.content = content;
            this.revision = revision;
            this.lastUpdateTime = lastUpdateTime;
        }
    }

    /**
     * 同步记录
     */
    static class SyncRecord {
        final String dataKey;
        final String target;
        final long revision;
        final boolean success;
        final long timestamp;

        SyncRecord(String dataKey, String target, long revision, boolean success) {
            this.dataKey = dataKey;
            this.target = target;
            this.revision = revision;
            this.success = success;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * 校验记录
     */
    static class VerifyRecord {
        final String dataKey;
        final String target;
        final long revision;
        final boolean matched;
        final boolean compensated;
        final long timestamp;

        VerifyRecord(String dataKey, String target, long revision, boolean matched, boolean compensated) {
            this.dataKey = dataKey;
            this.target = target;
            this.revision = revision;
            this.matched = matched;
            this.compensated = compensated;
            this.timestamp = System.currentTimeMillis();
        }
    }
}
