package com.alibaba.nacos.example.listener;

import com.alibaba.cloud.nacos.annotation.NacosConfigListener;
import com.alibaba.nacos.example.model.UserConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@link NacosConfigListener} 注解方法监听演示 — 观察者模式的最佳实践
 *
 * 核心设计思想：观察者模式 + 声明式监听 + 类型安全回调
 *
 * 一、观察者模式解耦（Observer Pattern）
 *   @NacosConfigListener 将被动轮询升级为事件驱动回调：
 *
 *   ┌───────────────────┐          ┌───────────────────────┐
 *   │  Nacos Server     │  gRPC    │ NacosConfigAnnotation │
 *   │  (Subject)        │  Push    │ Processor             │
 *   │                   │────────▶│ (Observer Manager)    │
 *   │  user-config.json │          │                       │
 *   │  scores.json      │          │ 管理所有 Listener 注册  │
 *   │  app-settings.    │          └───────────┬───────────┘
 *   │  properties       │                      │ 方法匹配 + 类型转换
 *   └───────────────────┘          ┌───────────┼───────────┐
 *                                  ▼           ▼           ▼
 *                           onUserConfig    onScores    onFullContent
 *                           Changed(User)   Changed(int[]) Changed(String)
 *                           ───────────── Observer 回调方法 ─────────
 *
 *   关键解耦点：
 *   1. 监听方法与配置源解耦 — Nacos Server 地址/协议由框架管理
 *   2. 监听方法与业务逻辑解耦 — 通过回调参数直接拿到最新对象
 *   3. 类型安全 — 框架自动将 JSON/Properties 反序列化为方法参数类型
 *
 * 二、声明式编程（Declarative）
 *   // 传统方式（命令式）：
 *   ConfigService configService = ...;
 *   configService.addListener(dataId, group, new Listener() {
 *       public void receiveConfigInfo(String configInfo) {
 *           UserConfig obj = JSON.parseObject(configInfo, UserConfig.class);  // ← 手动解析
 *           doBusinessLogic(obj);
 *       }
 *   });
 *
 *   // @NacosConfigListener 方式（声明式）：
 *   @NacosConfigListener(dataId = "user-config.json", group = "DEFAULT_GROUP")
 *   public void onUserConfigChanged(UserConfig config) {   // ← 自动反序列化
 *       this.latestUserConfig = config;                    // ← 直接使用强类型对象
 *   }
 *
 * 三、完整调用链路（断点调试指南）
 *   【阶段一：Bean 初始化 — 注册监听器】
 *   NacosConfigListenerDemo 构造
 *     → NacosConfigAnnotationProcessor.postProcessAfterInitialization()
 *       → 反射扫描所有 @NacosConfigListener 方法
 *       → 为每个方法构建 NacosConfigListenerWrapper
 *       → AbstractConfigChangeListener.onApplicationEvent()
 *       → ConfigService.addListener(dataId, group, wrapper)  ← 断点①：监听器注册
 *         wrapper 内部封装：方法签名 → 参数类型 → 反序列化策略
 *       → gRpc ClientStream 建立双向流长连接
 *
 *   【阶段二：配置变更 — 触发回调】
 *   Nacos 控制台修改 user-config.json → 发布
 *     → Nacos Server: RpcConfigChangeNotifier.configDataChanged()
 *     → gRPC BiRequestStream.onNext(ConfigChangeNotifyRequest) ← 断点②：接收推送通知
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
 *     → executeConfigListen()
 *     → checkListenCache() → MD5(content) 与本地缓存对比  ← 断点③：MD5 判断是否真的变更
 *     → refreshContentAndCheck()
 *     → listener.receiveConfigInfo(newContent)              ← 断点④：传入最新配置文本
 *     → NacosConfigListenerWrapper.receiveConfigInfo()
 *       → 根据方法参数类型选择反序列化器：
 *         String → 直接透传
 *         Properties → new Properties(content)
 *         JSON → Jackson/Gson 反序列化 → UserConfig / List<UserConfig> / Map<>
 *       → Method.invoke(bean, convertedArgs)                ← 断点⑤：调用 @NacosConfigListener 方法
 *
 *   【阶段三：initNotify 初始化回调】
 *   initNotify=true 时：
 *     → 在 addListener 注册后立即触发一次回调
 *     → 确保方法在启动时就拿到当前配置值
 *
 * 四、支持的参数类型一览
 * +----------------------------+----------------------------+----------------------------------+
 * | 方法参数类型                | dataId 格式要求            | 示例配置内容                      |
 * +----------------------------+----------------------------+----------------------------------+
 * | String                     | 任意                       | 原始文本内容                      |
 * | int/long/float/double/boolean| .properties 且需指定 key | app.debug=true                   |
 * | int[] / long[] 等          | .json                      | [95, 88, 76]                     |
 * | Properties                 | .properties / .yml         | a=1\nb=2                         |
 * | 自定义 Bean                | .json                      | {"username":"admin"}             |
 * | List<自定义Bean>           | .json                      | [{"username":"a"},{"username":"b"}]|
 * | Map<K,自定义Bean>          | .json                      | {"k1":{"name":"a"},"k2":{"name":"b"}}|
 * +----------------------------+----------------------------+----------------------------------+
 *
 * @author nacos-examples
 */
@Component
public class NacosConfigListenerDemo {

    private static final Logger log = LoggerFactory.getLogger(NacosConfigListenerDemo.class);

    /**
     * 变更计数器 — 用于验证配置变更确实触发了回调
     */
    private int changeCount = 0;

    /**
     * 最近一次变更时间
     */
    private LocalDateTime lastChangeTime;

    /**
     * 缓存的配置最新值（供外部查询）
     */
    private volatile String latestFullContent;
    private volatile int latestScore;
    private volatile UserConfig latestUserConfig;
    private volatile List<UserConfig> latestUserList;
    private volatile Properties latestProperties;

    // ================================================================
    // 监听类型一：完整配置文本变更（String 参数）
    // 适用场景：记录审计日志 / 同步到其他系统
    //
    // 对应 Nacos 配置：
    //   dataId: app-settings.properties, group: DEFAULT_GROUP
    //   initNotify=true → 启动时立即回调一次，获取当前配置
    // ================================================================

    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP",
            initNotify = true)
    public void onAppSettingsFullContentChanged(String content) {
        // === 断点⑥：回调入口，content 为最新完整配置文本 ===
        changeCount++;
        lastChangeTime = LocalDateTime.now();
        latestFullContent = content;

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] 完整配置变更                  ║");
        log.info("║  监听类型：String（完整文本）                               ║");
        log.info("║  dataId: app-settings.properties                        ║");
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("║  触发时间: {}                                    ║",
                lastChangeTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        log.info("║  新配置内容:                                             ║");
        log.info("║  ┌──────────────────────────────────────────────────┐   ║");
        if (content != null) {
            for (String line : content.split("\\n")) {
                log.info("║  │ {:<48s}│   ║", truncate(line, 48));
            }
        }
        log.info("║  └──────────────────────────────────────────────────┘   ║");
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 监听类型二：properties 中指定 key 的 int 值变更
    // 适用场景：阈值/开关类配置的精确监听
    //
    // 对应 Nacos 配置：
    //   dataId: app-settings.properties, group: DEFAULT_GROUP
    //   key: app.max-connections
    // ================================================================

    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP",
            key = "app.max-connections")
    public void onMaxConnectionsChanged(int maxConnections) {
        changeCount++;
        lastChangeTime = LocalDateTime.now();

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] key 值变更                    ║");
        log.info("║  监听类型：int（指定 key）                                  ║");
        log.info("║  key: app.max-connections                                ║");
        log.info("║  新值: {}                                               ║", maxConnections);
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 监听类型三：JSON 数组 → int[] 变更
    // 适用场景：分数阈值 / ID 列表监听
    //
    // 对应 Nacos 配置：
    //   dataId: scores.json, group: DEFAULT_GROUP
    //   内容: [95, 88, 76, 92, 100]
    // ================================================================

    @NacosConfigListener(dataId = "scores.json", group = "DEFAULT_GROUP")
    public void onScoresChanged(int[] scores) {
        changeCount++;
        lastChangeTime = LocalDateTime.now();

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] JSON数组 → int[] 变更         ║");
        log.info("║  dataId: scores.json                                     ║");
        log.info("║  新值: {}                                       ║",
                scores != null ? Arrays.toString(scores) : "null");

        // 业务处理示例：更新缓存值
        if (scores != null) {
            latestScore = scores.length > 0 ? scores[0] : 0;
            log.info("║  已更新 latestScore = {}                               ║", latestScore);
        }
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 监听类型四：JSON 对象 → UserConfig 变更
    // 适用场景：业务配置对象变更时触发下游逻辑
    //
    // 对应 Nacos 配置：
    //   dataId: user-config.json, group: DEFAULT_GROUP
    //   内容: {"username":"admin","password":"123456","age":25,...}
    // ================================================================

    @NacosConfigListener(dataId = "user-config.json", group = "DEFAULT_GROUP")
    public void onUserConfigChanged(UserConfig userConfig) {
        // === 断点⑤：方法被调用，userConfig 已经是反序列化后的强类型对象 ===
        changeCount++;
        lastChangeTime = LocalDateTime.now();

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] JSON → UserConfig 变更       ║");
        log.info("║  dataId: user-config.json                                ║");
        log.info("║  新值: {}                                   ║", userConfig);

        // 业务处理示例：验证年龄变更
        if (this.latestUserConfig != null && userConfig != null) {
            int oldAge = this.latestUserConfig.getAge();
            int newAge = userConfig.getAge();
            log.info("║  年龄变更: {} → {} (diff={})                           ║",
                    oldAge, newAge, newAge - oldAge);
        }

        // 更新缓存（volatile 保证可见性）
        this.latestUserConfig = userConfig;
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 监听类型五：JSON 数组 → List<UserConfig> 变更
    // 适用场景：用户列表/白名单配置变更
    //
    // 对应 Nacos 配置：
    //   dataId: user-list.json, group: DEFAULT_GROUP
    //   内容: [{"username":"admin","age":25},{"username":"user1","age":30}]
    // ================================================================

    @NacosConfigListener(dataId = "user-list.json", group = "DEFAULT_GROUP")
    public void onUserListChanged(List<UserConfig> userList) {
        changeCount++;
        lastChangeTime = LocalDateTime.now();

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] JSON数组 → List<UserConfig>  ║");
        log.info("║  dataId: user-list.json                                  ║");
        if (this.latestUserList != null) {
            log.info("║  列表大小变更: {} → {}                                ║",
                    this.latestUserList.size(),
                    userList != null ? userList.size() : 0);
        }
        log.info("║  新值: {}                                       ║", userList);
        this.latestUserList = userList;
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 监听类型六：Properties 对象变更
    // 适用场景：批量配置变更时的增量对比
    //
    // 对应 Nacos 配置：
    //   dataId: app-settings.properties, group: DEFAULT_GROUP
    // ================================================================

    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    public void onPropertiesChanged(Properties properties) {
        changeCount++;
        lastChangeTime = LocalDateTime.now();
        this.latestProperties = properties;

        log.info("");
        log.info("╔══════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfigListener 回调] Properties 对象变更          ║");
        log.info("║  dataId: app-settings.properties                         ║");
        log.info("║  当前属性数量: {}                                        ║",
                properties != null ? properties.size() : 0);
        log.info("║  触发序号: #{}                                         ║", changeCount);
        log.info("╚══════════════════════════════════════════════════════════╝");
    }

    // ================================================================
    // 初始化
    // ================================================================

    @PostConstruct
    public void init() {
        log.info("[NacosConfigListenerDemo] 初始化完成，所有 @NacosConfigListener 方法已注册为观察者");
        log.info("[NacosConfigListenerDemo] 等待 Nacos Server 配置变更推送...");
    }

    // ================================================================
    // Getter（供 Controller 查询）
    // ================================================================

    public int getChangeCount() {
        return changeCount;
    }

    public LocalDateTime getLastChangeTime() {
        return lastChangeTime;
    }

    public String getLatestFullContent() {
        return latestFullContent;
    }

    public int getLatestScore() {
        return latestScore;
    }

    public UserConfig getLatestUserConfig() {
        return latestUserConfig;
    }

    public List<UserConfig> getLatestUserList() {
        return latestUserList;
    }

    public Properties getLatestProperties() {
        return latestProperties;
    }

    public Map<String, Object> getListenerSnapshot() {
        Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
        snapshot.put("changeCount", changeCount);
        snapshot.put("lastChangeTime",
                lastChangeTime != null ? lastChangeTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) : null);
        snapshot.put("latestFullContent", latestFullContent);
        snapshot.put("latestScore", latestScore);
        snapshot.put("latestUserConfig", latestUserConfig != null ? latestUserConfig.toString() : null);
        snapshot.put("latestUserList", latestUserList);
        snapshot.put("latestProperties", latestProperties != null ? latestProperties.toString() : null);
        return snapshot;
    }

    // ================================================================
    // 辅助方法
    // ================================================================

    private static String truncate(String s, int maxLen) {
        if (s == null) return "null";
        return s.length() <= maxLen ? s : s.substring(0, maxLen - 3) + "...";
    }
}
