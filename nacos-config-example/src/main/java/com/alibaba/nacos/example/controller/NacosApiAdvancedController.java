package com.alibaba.nacos.example.controller;

import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.example.config.LocalConfigSnapshotDemo;
import com.alibaba.nacos.example.listener.GrpcPushReceiveDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Nacos 配置中心高级 API 演示 Controller
 *
 * 对应文章：
 *   - (四)：配置中心服务端 —— 事件总线与数据持久化
 *   - (五)：gRPC 推送链路 —— 配置变更下发与动态刷新
 *   - (六)：三级缓存体系 —— 降级兜底与故障自愈机制
 *
 * @author qinyu
 */
@RestController
@RequestMapping("/nacos")
public class NacosApiAdvancedController {

    private static final Logger log = LoggerFactory.getLogger(NacosApiAdvancedController.class);

    @Autowired
    private NacosConfigManager nacosConfigManager;

    @Autowired
    private LocalConfigSnapshotDemo localConfigSnapshotDemo;

    @Autowired
    private GrpcPushReceiveDemo grpcPushReceiveDemo;

    /**
     * 高级配置发布（支持指定配置类型）
     *
     * 对应 ConfigOperationService.publishConfig() 的多种发布模式：
     * - 普通发布：insertOrUpdate()
     * - CAS 发布：insertOrUpdateCas()
     *
     * POST /nacos/publish-advanced?dataId=xx&group=DEFAULT_GROUP&type=yaml
     * Body: 配置内容
     */
    @PostMapping("/publish-advanced")
    public Map<String, Object> publishConfigAdvanced(
            @RequestParam(defaultValue = "test-config.yml") String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group,
            @RequestParam(defaultValue = "yaml") String type,
            @RequestBody String content) {

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dataId", dataId);
        result.put("group", group);
        result.put("type", type);
        result.put("contentLength", content.length());

        try {
            ConfigService configService = nacosConfigManager.getConfigService();
            boolean success = configService.publishConfig(dataId, group, content, type);
            result.put("success", success);
            result.put("message", success ? "配置发布成功" : "配置发布失败");

            if (success) {
                log.info("配置发布成功: dataId={}, group={}, type={}, 内容长度={}", dataId, group, type, content.length());
                log.info("服务端触发链路:");
                log.info("  → ConfigOperationService.publishConfig()");
                log.info("    → configInfoPersistService.insertOrUpdate() 持久化");
                log.info("    → ConfigChangePublisher.notifyConfigChange(ConfigDataChangeEvent)");
                log.info("    → NotifyCenter 事件总线广播");
                log.info("      ├─ DumpService → NacosDelayTaskExecuteEngine 异步调度");
                log.info("      ├─ AsyncNotifyService → 集群节点同步");
                log.info("      └─ RpcConfigChangeNotifier → gRPC 双向流推送");
            }
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
            log.error("配置发布异常", e);
        }

        return result;
    }

    /**
     * 删除配置
     *
     * DELETE /nacos/remove?dataId=xx&group=DEFAULT_GROUP
     */
    @DeleteMapping("/remove")
    public Map<String, Object> removeConfig(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) {

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dataId", dataId);
        result.put("group", group);

        try {
            ConfigService configService = nacosConfigManager.getConfigService();
            boolean success = configService.removeConfig(dataId, group);
            result.put("success", success);
            result.put("message", success ? "配置已删除" : "配置不存在或删除失败");
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }

        return result;
    }

    /**
     * 三级缓存读取演示
     *
     * 模拟 Nacos 客户端配置读取的三级缓存策略：
     *   Failover → gRPC 远程 → Snapshot
     *
     * GET /nacos/cascade-fetch?dataId=dynamic-config.yml&group=DEFAULT_GROUP
     */
    @GetMapping("/cascade-fetch")
    public Map<String, Object> cascadeFetch(
            @RequestParam(defaultValue = "dynamic-config.yml") String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) {

        return localConfigSnapshotDemo.simulateThreeLevelRead(dataId, group);
    }

    /**
     * 查看本地缓存状态
     *
     * GET /config/cache-stats
     */
    @GetMapping("/config/cache-stats")
    public Map<String, Object> cacheStats() {
        return localConfigSnapshotDemo.getCacheStats();
    }

    /**
     * 注册 gRPC 推送监听器，查看推送统计
     *
     * POST /nacos/register-push-listener?dataId=dynamic-config.yml&group=DEFAULT_GROUP
     */
    @PostMapping("/register-push-listener")
    public Map<String, Object> registerPushListener(
            @RequestParam(defaultValue = "dynamic-config.yml") String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) {

        Map<String, Object> result = new LinkedHashMap<>();
        try {
            grpcPushReceiveDemo.registerListener(dataId, group, false);
            result.put("success", true);
            result.put("message", "gRPC 推送监听器已注册");
            result.put("dataId", dataId);
            result.put("group", group);
        } catch (NacosException e) {
            result.put("success", false);
            result.put("error", e.getMessage());
        }
        return result;
    }

    /**
     * gRPC 推送统计
     *
     * GET /nacos/push-stats
     */
    @GetMapping("/push-stats")
    public Map<String, Object> pushStats() {
        GrpcPushReceiveDemo.PushStats stats = grpcPushReceiveDemo.getStats();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalPushes", stats.totalPushes);
        result.put("lastPushTime", stats.lastPushTime);
        result.put("lastContentLength", stats.lastContent != null ? stats.lastContent.length() : 0);
        return result;
    }

    /**
     * 查看 Server 健康状态
     *
     * GET /nacos/server-status
     */
    @GetMapping("/server-status")
    public Map<String, Object> serverStatus() {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            ConfigService configService = nacosConfigManager.getConfigService();
            String status = configService.getServerStatus();
            result.put("serverStatus", status);
            result.put("connected", "UP".equalsIgnoreCase(status));
        } catch (Exception e) {
            result.put("serverStatus", "DOWN");
            result.put("connected", false);
            result.put("error", e.getMessage());
        }
        return result;
    }
}
