package com.alibaba.nacos.example.config;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 三级缓存与本地快照演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (六)：三级缓存体系 —— 降级兜底与故障自愈机制》
 *
 * Nacos 客户端在读取配置时采用三级缓存策略：
 *   ① Failover 容灾文件（最高优先级）- 用户手动放置，Server 不可用时的强制兜底
 *   ② 远程 gRPC 查询（正常路径）- ConfigRpcTransportClient.queryConfigInner()
 *   ③ 本地 Snapshot 快照（最后防线）- LocalConfigInfoProcessor.getSnapshot()
 *
 * 本类模拟这一策略，通过 ConfigService API 展示配置获取的全过程，
 * 并在无法连接 Server 时演示快照降级行为。
 *
 * 本地快照存储路径（参考 LocalConfigInfoProcessor）：
 *   ${user.home}/nacos/config/{envName}_nacos/snapshot/{group}/{dataId}
 * 容灾文件路径：
 *   ${user.home}/nacos/config/{serverName}_nacos/data/config-data/{group}/{dataId}
 *
 * @author qinyu
 */
@Component
public class LocalConfigSnapshotDemo {

    private static final Logger log = LoggerFactory.getLogger(LocalConfigSnapshotDemo.class);

    @Autowired
    private ConfigService configService;

    /**
     * 缓存状态统计
     */
    private final ConcurrentHashMap<String, CacheEntry> cacheStats = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("====== LocalConfigSnapshotDemo 初始化 ======");
        log.info("组件已就绪，支持三级缓存读取策略演示");
        log.info("===========================================");
    }

    /**
     * 模拟三级缓存读取（仅演示读取效果）
     *
     * @param dataId 配置 ID
     * @param group  配置分组
     * @return 包含各级读取结果的 Map
     */
    public Map<String, Object> simulateThreeLevelRead(String dataId, String group) {
        Map<String, Object> result = new LinkedHashMap<>();
        long startTime = System.currentTimeMillis();

        // 第一级：Failover 容灾文件检查
        String failoverPath = getFailoverFilePath(dataId, group);
        File failoverFile = new File(failoverPath);
        boolean failoverExists = failoverFile.exists() && failoverFile.isFile();

        result.put("level1_failover_path", failoverPath);
        result.put("level1_failover_exists", failoverExists);
        if (failoverExists) {
            try {
                String content = new String(Files.readAllBytes(failoverFile.toPath()));
                result.put("level1_failover_hint", "已命中容灾文件，返回本地内容（约 " + content.length() + " 字节）");
            } catch (IOException e) {
                result.put("level1_failover_hint", "容灾文件读取失败: " + e.getMessage());
            }
        } else {
            result.put("level1_failover_hint", "未命中容灾文件，降级到远程查询");
        }

        // 第二级：远程 gRPC 查询（实际调用 ConfigService API）
        result.put("level2_remote_hint", "通过 gRPC 向 Nacos Server 发起 ConfigService.getConfig() 查询");
        try {
            String remoteContent = configService.getConfig(dataId, group, 3000L);
            if (remoteContent != null) {
                result.put("level2_remote_success", true);
                result.put("level2_remote_content_length", remoteContent.length());
                result.put("level2_remote_md5", md5(remoteContent));
                // 远程成功时会自动写入本地快照（由 Nacos Client 内部完成）
                result.put("level2_remote_note", "远程查询成功，Nacos Client 已自动写入本地快照");
                recordCacheHit(dataId, group, "remote", true);
            } else {
                result.put("level2_remote_success", false);
                result.put("level2_remote_note", "远程查询返回空（配置不存在）");
                result.put("level2_snapshot_hint", "配置不存在，尝试从快照恢复...");
                recordCacheHit(dataId, group, "remote", false);
            }
        } catch (NacosException e) {
            result.put("level2_remote_success", false);
            result.put("level2_remote_error", e.getMessage());
            result.put("level2_remote_note", "远程查询失败，降级到本地快照");

            // 第三级：本地 Snapshot 快照兜底
            String snapshotContent = readSnapshot(dataId, group);
            if (snapshotContent != null) {
                result.put("level3_snapshot_hit", true);
                result.put("level3_snapshot_content_length", snapshotContent.length());
                result.put("level3_snapshot_md5", md5(snapshotContent));
                result.put("level3_snapshot_note", "本地快照命中（最后一次成功拉取的数据）");
            } else {
                result.put("level3_snapshot_hit", false);
                result.put("level3_snapshot_note", "无可用本地快照，返回 null");
            }
            recordCacheHit(dataId, group, "snapshot", snapshotContent != null);
        }

        result.put("elapsed_ms", System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * 缓存状态快照
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("total_cached_configs", cacheStats.size());
        stats.put("entries", new LinkedHashMap<>(cacheStats));
        return stats;
    }

    /**
     * 记录缓存命中状态
     */
    private void recordCacheHit(String dataId, String group, String source, boolean hit) {
        String key = group + "@@" + dataId;
        cacheStats.compute(key, (k, v) -> {
            if (v == null) {
                v = new CacheEntry(dataId, group);
            }
            if ("remote".equals(source)) {
                v.remoteHitCount++;
                v.lastRemoteHit = hit;
            } else {
                v.snapshotHitCount++;
                v.lastSnapshotHit = hit;
            }
            v.lastAccessTime = System.currentTimeMillis();
            return v;
        });
    }

    /**
     * 读取本地快照文件
     */
    private String readSnapshot(String dataId, String group) {
        String snapshotPath = getSnapshotFilePath(dataId, group);
        File file = new File(snapshotPath);
        if (file.exists() && file.isFile()) {
            try {
                return new String(Files.readAllBytes(file.toPath()));
            } catch (IOException e) {
                log.warn("读取本地快照文件失败: {}", snapshotPath, e);
            }
        }
        return null;
    }

    /**
     * 构造快照文件路径（与 LocalConfigInfoProcessor 一致）
     */
    private String getSnapshotFilePath(String dataId, String group) {
        String userHome = System.getProperty("user.home");
        return userHome + "/nacos/config/nacos-config-example_nacos/snapshot/" + group + "/" + dataId;
    }

    /**
     * 构造容灾文件路径
     */
    private String getFailoverFilePath(String dataId, String group) {
        String userHome = System.getProperty("user.home");
        return userHome + "/nacos/config/nacos-config-example_nacos/data/config-data/" + group + "/" + dataId;
    }

    /**
     * 简易 MD5（仅用于演示长度对比，非真实 MD5）
     */
    private String md5(String content) {
        return String.format("%032x", content.length());
    }

    /**
     * 缓存条目
     */
    static class CacheEntry {
        final String dataId;
        final String group;
        long remoteHitCount;
        long snapshotHitCount;
        boolean lastRemoteHit;
        boolean lastSnapshotHit;
        long lastAccessTime;

        CacheEntry(String dataId, String group) {
            this.dataId = dataId;
            this.group = group;
        }
    }
}
