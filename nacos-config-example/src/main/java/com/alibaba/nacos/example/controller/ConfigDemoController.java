package com.alibaba.nacos.example.controller;

import com.alibaba.nacos.example.config.DynamicConfigProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 配置演示控制器 — 展示 @RefreshScope 动态刷新效果
 *
 * 对应文章核心机制：
 * - 配置获取（一、客户端启动）：
 *   应用启动时，NacosConfigDataLoader 从 Nacos Server 拉取配置并注入 Environment；
 *   本控制器中 @Value 注入的 app.name、app.version 即为拉取到的远程配置值
 * - 动态刷新（三、客户端接收）：
 *   Nacos 控制台修改配置 → gRPC 推送 → NacosConfigRefreshEvent
 *   → NacosConfigRefreshEventListener 转换 → RefreshEvent → @RefreshScope 重建 Bean
 *   → 本控制器中的 appName / appVersion 自动更新为新值
 *
 * 对应源码链路：
 *   RpcConfigChangeNotifier.configDataChanged()  // 服务端推送
 *     → GrpcClient.bindRequestStream().onNext()  // 客户端 gRPC 双向流接收
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()  // 标记缓存失效
 *     → executeConfigListen() → checkListenCache()  // 批量 MD5 校验 + 拉取
 *     → refreshContentAndCheck() → listener.receiveConfigInfo()
 *     → NacosContextRefresher.innerReceive() → NacosConfigRefreshEvent
 *     → NacosConfigRefreshEventListener.onApplicationEvent() → RefreshEvent
 *     → @RefreshScope Bean 重建 → 新配置生效
 *
 * @author nacos-examples
 */
@RestController
@RequestMapping("/config")
@RefreshScope
public class ConfigDemoController {

    private static final Logger log = LoggerFactory.getLogger(ConfigDemoController.class);

    // ============================================================
    // 通过 @Value 注入配置属性（会被 Nacos 远程配置覆盖）
    // 对应文章：NacosConfigDataLoader.load() → Environment → @Value 注入
    // ============================================================

    /**
     * 从配置中注入 app.name
     * 对照文章 NacosConfigDataLoader.load() 拉取配置 → 注入 Environment → @Value 解析
     */
    @Value("${app.name:nacos-config-demo}")
    private String appName;

    /**
     * 从配置中注入 app.version
     * @RefreshScope 保证修改 Nacos 配置后该值自动刷新
     */
    @Value("${app.version:1.0.0}")
    private String appVersion;

    // ============================================================
    // 通过 @ConfigurationProperties 注入（同样支持 @RefreshScope）
    // ============================================================

    private final DynamicConfigProperties configProperties;

    public ConfigDemoController(DynamicConfigProperties configProperties) {
        this.configProperties = configProperties;
    }

    @PostConstruct
    public void init() {
        log.info("====== ConfigDemoController 初始化 ======");
        log.info("appName (from @Value): {}", appName);
        log.info("appVersion (from @Value): {}", appVersion);
        log.info("configProperties.name: {}", configProperties.getName());
        log.info("configProperties.version: {}", configProperties.getVersion());
        log.info("==========================================");
    }

    /**
     * 查看当前生效的完整配置信息
     *
     * 调用此接口可以对比 @Value 注入和 @ConfigurationProperties 绑定两种方式的配置值。
     * 在 Nacos 控制台修改配置后再次调用，可验证 @RefreshScope 动态刷新效果。
     *
     * 实际操作中，配置通过以下路径生效：
     *   Nacos 控制台发布 → RpcConfigChangeNotifier 推送
     *     → ConfigRpcTransportClient 处理 → executeConfigListen()
     *     → NacosContextRefresher 发布事件 → RefreshEvent
     *     → ContextRefresher.refresh() → Environment 重新加载
     *     → @RefreshScope Bean 销毁重建 → @Value 和 @ConfigurationProperties 获取新值
     *
     * @return 包含所有配置信息的 Map
     */
    @GetMapping("/info")
    public Map<String, Object> getConfigInfo() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        // @Value 注入方式
        Map<String, Object> valueInjection = new LinkedHashMap<>();
        valueInjection.put("appName", appName);
        valueInjection.put("appVersion", appVersion);
        result.put("@Value注入（@RefreshScope）", valueInjection);

        // @ConfigurationProperties 绑定方式
        Map<String, Object> propertiesBinding = new LinkedHashMap<>();
        propertiesBinding.put("name", configProperties.getName());
        propertiesBinding.put("version", configProperties.getVersion());
        propertiesBinding.put("refreshInterval", configProperties.getRefreshInterval());
        result.put("@ConfigurationProperties绑定（@RefreshScope）", propertiesBinding);

        return result;
    }

    /**
     * 直接查看 @Value 注入的 appName
     * 用于快速验证动态刷新是否生效
     */
    @GetMapping("/app-name")
    public Map<String, String> getAppName() {
        return Map.of("appName", appName, "version", appVersion);
    }
}
