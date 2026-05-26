package com.alibaba.nacos.example.controller;

import com.alibaba.cloud.nacos.NacosConfigManager;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Nacos ConfigService API 直接调用演示控制器
 *
 * 本控制器不依赖 Spring Cloud 自动装配机制，直接通过 NacosConfigManager 获取
 * ConfigService 实例，手动调用底层 API。对应文章讲解的核心接口和类：
 *
 * 对应文章核心机制：
 * +-----------------------------------------+--------------------------------------------------------+-----------------------------------+
 * | 接口/方法                                | 文章对应章节                                            | 核心作用                           |
 * +-----------------------------------------+--------------------------------------------------------+-----------------------------------+
 * | ConfigService.getConfig()               | 一、客户端启动 → "底层通信入口：ConfigService"           | 一次性拉取配置（不监听后续变更）     |
 * | ConfigService.getConfigAndSignListener()| 一 → ConfigService 接口方法                             | 原子性获取配置并注册监听器，避免时序间隙 |
 * | ConfigService.addListener()             | 一 → "监听器的注册入口：NacosContextRefresher"           | 注册配置变更监听器，底层通过 gRPC 双向流订阅 |
 * | ConfigService.publishConfig()           | 二 → "配置变更的触发入口：ConfigOperationService"        | 向 Nacos Server 发布配置（普通发布） |
 * | ConfigService.publishConfigCas()        | 二 → ConfigOperationService CAS 机制                    | 带乐观锁的配置发布（Compare-And-Swap） |
 * | ConfigService.removeConfig()            | 一 → ConfigService 接口方法                             | 删除 Nacos Server 上的配置         |
 * +-----------------------------------------+--------------------------------------------------------+-----------------------------------+
 *
 * 对应源码链路（addListener 为例）：
 *   addListener(dataId, group, listener)
 *     → NacosConfigService.addListener()
 *     → ClientWorker.addTenantListeners()
 *     → ConfigRpcTransportClient.executeConfigListen()
 *     → checkListenCache()  // 批量 MD5 比对
 *     → 服务端推送 → GrpcClient.onNext()
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
 *     → executeConfigListen() → checkListenCache()
 *     → refreshContentAndCheck() → listener.receiveConfigInfo(newContent)
 *
 * @author nacos-examples
 */
@RestController
@RequestMapping("/nacos")
public class NacosApiController {

    private static final Logger log = LoggerFactory.getLogger(NacosApiController.class);

    /**
     * NacosConfigManager 由 NacosConfigAutoConfiguration 自动装配，
     * 内部维护了 NacosConfigService 实例（ConfigService 的默认实现），
     * 底层通过 gRPC 双向流（BiRequestStream）与 Nacos Server 通信
     *
     * 对应文章 NacosConfigSpringCloudAutoConfiguration 中注入的 NacosConfigManager
     */
    @Autowired
    private NacosConfigManager nacosConfigManager;

    /**
     * 从 NacosConfigManager 中获取 ConfigService 实例
     * 对应文章 NacosConfigDataLoader.load() 中
     * NacosConfigManager.getConfigService() 的调用方式
     */
    private ConfigService getConfigService() {
        return nacosConfigManager.getConfigService();
    }

    /**
     * 记录每个 dataId 的监听器重连次数（用于 addListener 演示）
     */
    private final ConcurrentHashMap<String, AtomicInteger> listenerTriggerCount = new ConcurrentHashMap<>();

    // ============================================================
    // 一、配置获取 — 对应文章 ConfigService.getConfig()
    // ============================================================

    /**
     * 从 Nacos Server 拉取配置内容（一次性操作）
     *
     * 对应源码链路：
     *   ConfigService.getConfig(dataId, group, timeoutMs)
     *     → ClientWorker.getServerConfig()
     *     → ConfigRpcTransportClient.queryConfigInner()
     *     → LocalConfigInfoProcessor.saveSnapshot()  // 写入本地快照
     *     → 返回配置内容
     *
     *   Server 端处理：
     *     → ConfigQueryRequestHandler.handle()
     *     → ConfigCacheService.getContentCache()  // 从内存缓存读取
     *     → ConfigDiskServiceFactory.getContent() // 从磁盘读取内容
     *
     * @param dataId 配置 Data ID
     * @param group  配置 Group（默认 DEFAULT_GROUP）
     * @param timeoutMs 超时时间（毫秒）
     * @return 配置内容字符串
     */
    @GetMapping("/fetch")
    public String fetchConfig(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group,
            @RequestParam(defaultValue = "5000") long timeoutMs) throws NacosException {

        log.info("====== 配置拉取：getConfig() ======");
        log.info("dataId: {}, group: {}, timeoutMs: {}ms", dataId, group, timeoutMs);

        // 调用 ConfigService.getConfig() — 同步拉取配置
        // 内部会先检查 failover 文件 → 远程 gRPC 请求 → 本地快照兜底
        String content = getConfigService().getConfig(dataId, group, timeoutMs);
        log.info("拉取结果: {}", content != null ? "成功 (长度: " + content.length() + ")" : "配置不存在");
        return content != null ? content : "配置不存在";
    }

    // ============================================================
    // 二、长轮询 / gRPC 推送监听 — 对应文章 addListener()
    // ============================================================

    /**
     * 为指定配置注册监听器（底层基于 gRPC 双向流通信推送）
     *
     * 这是 Nacos 动态刷新的核心 API。对应源码链路：
     *   addListener(dataId, group, listener)
     *     |
     *     +-- NacosConfigService.addTenantListenersWithContent()
     *     |     → ClientWorker.addTenantListeners()
     *     |     → ConfigRpcTransportClient 将 (dataId, group, listener) 加入缓存
     *     |     → 通过阻塞队列唤醒后台主循环
     *     |     → executeConfigListen() → checkListenCache()
     *     |     → 向服务端发送 ConfigBatchListenRequest (携带本地 MD5)
     *     |     → 服务端比对 MD5，返回变更列表
     *     |
     *     +-- [服务端推送时]
     *     |     → RpcConfigChangeNotifier.configDataChanged()
     *     |     → push() → rpcPushService.pushWithCallback()
     *     |     → gRPC BiRequestStream → GrpcClient.bindRequestStream().onNext()
     *     |     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
     *     |     → 标记 consistentWithServer=false → notifyListenConfig()
     *     |     → executeConfigListen() → checkListenCache()
     *     |     → refreshContentAndCheck() → listener.receiveConfigInfo(newContent)
     *     |
     *     +-- [3分钟全量兜底同步]
     *           → needAllSync=true → 所有缓存强制 MD5 校验
     *           → 防止增量推送信号丢失导致配置长期不一致
     *
     * 使用方法：
     * 1. POST /nacos/listen?dataId=dynamic-config.yml
     * 2. 在 Nacos 控制台修改 dynamic-config.yml 的配置内容
     * 3. 观察应用控制台日志：收到推送通知 → MD5 校验 → 拉取新内容 → 回调触发
     *
     * @param dataId 配置 Data ID
     * @param group  配置 Group
     * @return 注册结果描述
     */
    @PostMapping("/listen")
    public String addListener(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) throws NacosException {

        log.info("====== 注册配置监听器（gRPC 双向流推送） ======");
        log.info("dataId: {}, group: {}", dataId, group);

        // 先获取当前配置内容（确保监听器能立即感知到当前状态）
        String currentConfig = getConfigService().getConfig(dataId, group, 5000);

        // 为监听器设置独立的触发计数器
        String key = dataId + "@@@" + group;
        listenerTriggerCount.putIfAbsent(key, new AtomicInteger(0));

        // 注册监听器 — 底层通过 gRPC 双向流向服务端订阅
        // 对应 NacosContextRefresher.registerNacosListener() 中 AbstractSharedListener 的实现
        getConfigService().addListener(dataId, group, new Listener() {

            /**
             * 配置变更回调 — 当服务端通过 gRPC 推送变更通知后，
             * 客户端拉取新内容并回调此方法
             *
             * @param configInfo 最新的配置内容
             */
            @Override
            public void receiveConfigInfo(String configInfo) {
                int count = listenerTriggerCount.get(key).incrementAndGet();
                log.info("================================================");
                log.info("[监听器回调 #{}] 配置已变更!", count);
                log.info("  dataId : {}", dataId);
                log.info("  group  : {}", group);
                log.info("  新内容长度: {} 字符", configInfo != null ? configInfo.length() : 0);
                log.info("  新内容预览: {}",
                        configInfo != null && configInfo.length() > 200
                                ? configInfo.substring(0, 200) + "..."
                                : configInfo);
                log.info("================================================");
            }

            /**
             * 返回执行回调的线程池
             * 返回 null 表示由 ConfigRpcTransportClient 的内部线程直接执行
             * 对应文章：ConfigRpcTransportClient.startInternal() 中的无限循环线程
             */
            @Override
            public Executor getExecutor() {
                return null;
            }
        });

        String result = String.format(
                "监听器注册成功！\n  dataId: %s\n  group: %s\n  " +
                "当前配置: %s\n  \n请在 Nacos 控制台修改此配置，观察日志中的回调输出。",
                dataId, group,
                currentConfig != null && currentConfig.length() > 100
                        ? currentConfig.substring(0, 100) + "..."
                        : currentConfig);

        log.info(result);
        return result;
    }

    /**
     * 使用 getConfigAndSignListener 原子操作获取配置并注册监听器
     *
     * 相比单独的 getConfig() + addListener()，此方法能避免获取配置后、
     * 注册监听前发生的变更丢失。
     *
     * 对应文章 ConfigService 接口中的第二个方法，
     * 原子性保证：配置获取和监听器注册在同一临界区内完成
     */
    @PostMapping("/listen-atomic")
    public String addListenerAtomic(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) throws NacosException {

        log.info("====== 原子性获取配置并注册监听器 ======");
        log.info("dataId: {}, group: {}", dataId, group);

        String key = dataId + "@@@" + group;
        listenerTriggerCount.putIfAbsent(key, new AtomicInteger(0));

        // getConfigAndSignListener 原子性更好
        String content = getConfigService().getConfigAndSignListener(dataId, group, 5000, new Listener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                int count = listenerTriggerCount.get(key).incrementAndGet();
                log.info("[原子监听回调 #{}] 配置变更! dataId={}, group={}, 新内容长度={}",
                        count, dataId, group, configInfo.length());
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        });

        return String.format("配置内容（已原子注册监听器）：\n%s", content);
    }

    // ============================================================
    // 三、配置发布 — 对应文章 ConfigOperationService.publishConfig()
    // ============================================================

    /**
     * 向 Nacos Server 发布配置（普通发布模式）
     *
     * 对应源码链路：
     *   publishConfig(dataId, group, content, type)
     *     → NacosConfigService.publishConfigInner()
     *     → ConfigRpcTransportClient 通过 gRPC 发送 ConfigPublishRequest
     *     → Server 端：ConfigController.publishConfig()
     *       → ConfigOperationService.publishConfig()
     *         → configInfoPersistService.insertOrUpdate()  // 持久化到 MySQL
     *         → ConfigChangePublisher.notifyConfigChange()  // 发布 ConfigDataChangeEvent
     *         → NotifyCenter 广播给订阅者：
     *           ├─ DumpService.handleConfigDataChange()     // 本地转储
     *           ├─ AsyncNotifyService.handleConfigDataChange() // 集群同步
     *           └─ RpcConfigChangeNotifier.onEvent()        // gRPC 推送客户端
     *
     * @param dataId  配置 Data ID
     * @param group   配置 Group
     * @param content 配置内容（YAML 格式字符串）
     * @param type    配置类型（yaml / json / properties / text）
     * @return 发布结果
     */
    @PostMapping("/publish")
    public String publishConfig(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group,
            @RequestBody String content,
            @RequestParam(defaultValue = "yaml") String type) throws NacosException {

        log.info("====== 发布配置（普通发布模式） ======");
        log.info("dataId: {}, group: {}, type: {}", dataId, group, type);
        log.info("content length: {}", content.length());

        // 调用 ConfigService.publishConfig() 发布配置
        // 底层通过 gRPC 发送 ConfigPublishRequest
        boolean success = getConfigService().publishConfig(dataId, group, content, type);

        log.info("发布结果: {}", success ? "成功" : "失败");
        return success ? "配置发布成功！所有监听该配置的客户端将收到 gRPC 推送通知。" : "配置发布失败！";
    }

    /**
     * CAS（Compare-And-Swap）乐观锁方式发布配置
     *
     * 对应文章 ConfigOperationService 中 casMd5 的并发控制机制：
     * 多人同时编辑同一配置时，后提交的人会被告知冲突并拒绝覆盖。
     *
     * 操作流程：
     * 1. 先调用 getConfig() 获取当前内容，计算其 MD5 值
     * 2. 编辑配置内容
     * 3. 调用 publishConfigCas()，传入编辑前内容的 MD5 作为 casMd5
     * 4. 服务端比对 MD5：相同则允许更新，不同则拒绝（说明有人抢先改过）
     */
    @PostMapping("/publish-cas")
    public String publishConfigCas(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group,
            @RequestBody String newContent,
            @RequestParam(defaultValue = "yaml") String type) throws NacosException {

        log.info("====== CAS 方式发布配置 ======");

        // 1. 先获取当前配置内容
        String currentContent = getConfigService().getConfig(dataId, group, 5000);

        // 2. 计算当前内容的 MD5（作为 casMd5 版本标识）
        String casMd5 = com.alibaba.nacos.api.common.Constants.NULL;
        if (currentContent != null) {
            casMd5 = DigestUtils.md5DigestAsHex(currentContent.getBytes(StandardCharsets.UTF_8));
        }
        log.info("当前配置 MD5 (casMd5): {}", casMd5);

        // 3. 带 CAS 乐观锁发布
        boolean success = getConfigService().publishConfigCas(dataId, group, newContent, casMd5, type);

        if (success) {
            return "CAS 发布成功！MD5 一致，配置已更新。";
        } else {
            return "CAS 发布失败！MD5 不一致，配置已被他人修改，请重新获取后再试。";
        }
    }

    // ============================================================
    // 四、配置删除
    // ============================================================

    /**
     * 删除 Nacos Server 上的配置
     */
    @DeleteMapping("/remove")
    public String removeConfig(
            @RequestParam String dataId,
            @RequestParam(defaultValue = "DEFAULT_GROUP") String group) throws NacosException {

        log.info("====== 删除配置 ======");
        log.info("dataId: {}, group: {}", dataId, group);

        boolean success = getConfigService().removeConfig(dataId, group);
        return success ? "配置删除成功！" : "配置删除失败！";
    }

    /**
     * 获取 Nacos Server 健康状态
     * 对应文章 ConfigService.getServerStatus()
     */
    @GetMapping("/health")
    public String getServerStatus() {
        String status = getConfigService().getServerStatus();
        log.info("Nacos Server 状态: {}", status);
        return "Nacos Server Status: " + status;
    }
}
