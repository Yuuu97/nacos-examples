package com.alibaba.nacos.example.config;

import com.alibaba.cloud.nacos.annotation.NacosConfig;
import com.alibaba.nacos.example.model.UserConfig;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * {@link NacosConfig} 注解字段注入演示 — 覆盖六种注入类型
 *
 * 核心设计思想：配置驱动 + 依赖注入 + 观察者模式
 *
 * 一、配置驱动（Configuration-Driven）
 *   传统方式：@Value("${key}")        → 仅支持字符串，需手动转换类型，需 @RefreshScope
 *   注解方式：@NacosConfig(dataId=..) → 自动类型转换 + JSON反序列化，无需 @RefreshScope
 *
 *   配置驱动意味着：应用行为由 Nacos 中的配置数据驱动，
 *   而非硬编码在 Java 源码中。修改配置即修改行为，零停机。
 *
 * 二、观察者模式（Observer Pattern）解耦
 *               ┌──────────────────────────────────┐
 *               │  Nacos Server（主题 / Subject）    │
 *               │  - user-config.json              │
 *               │  - app-settings.properties       │
 *               │  - scores.json                   │
 *               └──────────┬───────────────────────┘
 *                          │ gRPC BiRequestStream（双向长连接）
 *                          │ RpcConfigChangeNotifier.configDataChanged()
 *                          ▼
 *               ┌──────────────────────────────────┐
 *               │ NacosConfigAnnotationProcessor    │
 *               │ （观察者管理器 / Observer Manager） │
 *               └──────────┬───────────────────────┘
 *                          │ 字段值注入 / 对象重建
 *          ┌───────────────┼───────────────┐
 *          ▼               ▼               ▼
 *   configContent    booleanValue    userConfig
 *   (String)         (boolean)      (UserConfig)
 *   ───────────────── 观察者（Observers）─────────────
 *
 *   解耦机制：
 *   1. 观察者（@NacosConfig 字段）与主题（Nacos Server）完全解耦
 *   2. 字段所属的 Bean 无需实现任何接口或继承任何类
 *   3. 配置变更通过 gRPC 推送自动传播，无需手动轮询
 *
 * 三、完整调用链路（断点调试指南）
 *   【阶段一：启动加载】
 *   ApplicationRunner / @PostConstruct
 *     → NacosConfigAnnotationProcessor.postProcessAfterInitialization()
 *       → 反射扫描所有 @NacosConfig 字段
 *       → for each field:
 *           ConfigService.getConfig(dataId, group)     ← 断点①：观察首次拉取的配置内容
 *           → 类型转换（String/int/JSON反序列化）
 *           → ReflectionUtils.setField(field, bean, value) ← 断点②：确认字段注入的值
 *
 *   【阶段二：注册监听】
 *   → NacosConfigAnnotationProcessor 内部
 *     → AbstractConfigChangeListener.onApplicationEvent()
 *     → ConfigService.addListener(dataId, group, listener)  ← 断点③：确认监听注册
 *     → gRPC ClientStream.addListener() 建立双向流
 *
 *   【阶段三：变更触发】
 *   Nacos 控制台修改配置 → 发布
 *     → Nacos Server: RpcConfigChangeNotifier.configDataChanged()
 *     → gRPC BiRequestStream.onNext(ConfigChangeNotifyRequest) ← 断点④：捕获推送通知
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
 *     → executeConfigListen() → checkListenCache() → MD5 校验
 *     → refreshContentAndCheck() → listener.receiveConfigInfo(content) ← 断点⑤：最新配置内容
 *     → NacosConfigAnnotationProcessor 触发字段刷新 ← 断点⑥：观察字段新值
 *
 * @author qinyu
 */
@Component
public class NacosConfigAnnotationDemo {
    
    private static final Logger log = LoggerFactory.getLogger(NacosConfigAnnotationDemo.class);
    
    // ================================================================
    // 类型一：注入完整配置文本（String）
    // 适用场景：拿到原始内容自行解析 / 透传给第三方系统
    //
    // Nacos 配置：dataId=app-settings.properties, group=DEFAULT_GROUP
    // 内容示例：
    //   app.name=nacos-demo
    //   app.version=2.0.0
    //   app.debug=true
    // ================================================================
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    private String configFullContent;
    
    @NacosConfig(dataId = "application-dev.yml", group = "DEFAULT_GROUP")
    private String name;
    
    // ================================================================
    // 类型二：注入指定 key 的基础类型值（boolean / int / long / float / double）
    // 适用场景：精确获取 properties 文件中某个属性，自动类型转换
    //
    // key 指向 app-settings.properties 中的 app.debug 属性
    // defaultValue 在配置不存在时兜底
    // ================================================================
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP", key = "app.debug", defaultValue = "false")
    private boolean appDebugEnabled;
    
    // ================================================================
    // 类型三：注入 JSON 数组为基础类型数组（int[]）
    // 适用场景：批量数值配置（阈值列表、ID 集合等）
    //
    // Nacos 配置：dataId=scores.json, group=DEFAULT_GROUP
    // 内容示例：[95, 88, 76, 92, 100]
    // ================================================================
    @NacosConfig(dataId = "scores.json", group = "DEFAULT_GROUP")
    private int[] scores;
    
    // ================================================================
    // 类型四：注入 JSON 为自定义 JavaBean（UserConfig）
    // 适用场景：复杂业务配置对象化，强类型约束
    //
    // Nacos 配置：dataId=user-config.json, group=DEFAULT_GROUP
    // 内容示例：{"username":"admin","password":"123456","age":25,...}
    // ================================================================
    @NacosConfig(dataId = "user-config.json", group = "DEFAULT_GROUP")
    private UserConfig userConfig;
    
    // ================================================================
    // 类型五：注入 JSON 数组为 List<UserConfig>
    // 适用场景：多实体配置列表（用户列表、白名单等）
    //
    // Nacos 配置：dataId=user-list.json, group=DEFAULT_GROUP
    // 内容示例：[{"username":"admin","age":25},{"username":"user1","age":30}]
    // ================================================================
    @NacosConfig(dataId = "user-list.json", group = "DEFAULT_GROUP")
    private List<UserConfig> userConfigList;
    
    // ================================================================
    // 类型六：注入为 Properties 对象
    // 适用场景：遍历所有配置项 / 动态查找 key
    //
    // 支持 .properties 和 .yaml/.yml 格式
    // ================================================================
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    private Properties appSettings = new Properties();
    
    // ================================================================
    // 初始化回调：启动时打印所有注入的配置值
    // 对应调用链路【阶段一：启动加载】
    // ================================================================
    
    @PostConstruct
    public void init() {
        log.info("");
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  [@NacosConfig 字段注入] 启动加载完成                         ║");
        log.info("║  调用链路：@PostConstruct                                    ║");
        log.info("║    → NacosConfigAnnotationProcessor 扫描 @NacosConfig 字段   ║");
        log.info("║    → ConfigService.getConfig(dataId, group) 拉取远程配置      ║");
        log.info("║    → 类型转换 + 反射注入字段                                  ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");
        
        printAllConfigs();
    }
    
    // ================================================================
    // 打印当前所有 @NacosConfig 字段的生效值
    // 用于断点调试和热刷新验证
    // ================================================================
    
    /**
     * 打印所有 @NacosConfig 注入的当前值。配置变更后，这些字段引用的对象会被替换，再次调用本方法可观察到新值。
     *
     * 验证方法：
     * 1. 启动应用 → 观察启动日志中的初始配置值
     * 2. 在 Nacos 控制台修改 user-config.json（如 age: 25 → 30）
     * 3. 等待 gRPC 推送通知到达（通常 3-10 秒）
     * 4. 调用 GET /annotation/info → 确认 userConfig.age 已更新为 30
     */
    public void printAllConfigs() {
        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│ [类型一] configFullContent (完整文本)                          │");
        log.info("│   dataId=app-settings.properties                            │");
        log.info("│   value:\n{}", indent(configFullContent, "│     "));
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ [类型二] appDebugEnabled (指定key → boolean)                  │");
        log.info("│   key=app.debug, defaultValue=false                         │");
        log.info("│   value: {}", appDebugEnabled);
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ [类型三] scores (JSON数组 → int[])                            │");
        log.info("│   dataId=scores.json                                        │");
        log.info("│   value: {}", scores != null ? Arrays.toString(scores) : "null");
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ [类型四] userConfig (JSON对象 → UserConfig)                   │");
        log.info("│   dataId=user-config.json                                   │");
        log.info("│   value: {}", userConfig);
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ [类型五] userConfigList (JSON数组 → List<UserConfig>)         │");
        log.info("│   dataId=user-list.json                                     │");
        log.info("│   value: {}", userConfigList);
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ [类型六] appSettings (properties → Properties)                │");
        log.info("│   dataId=app-settings.properties                            │");
        log.info("│   value: {}", appSettings);
        log.info("└─────────────────────────────────────────────────────────────┘");
    }
    
    // ================================================================
    // Getter（供 Controller 查询当前值）
    // ================================================================
    
    public String getConfigFullContent() {
        return configFullContent;
    }
    
    public boolean isAppDebugEnabled() {
        return appDebugEnabled;
    }
    
    public int[] getScores() {
        return scores;
    }
    
    public UserConfig getUserConfig() {
        return userConfig;
    }
    
    public List<UserConfig> getUserConfigList() {
        return userConfigList;
    }
    
    public Properties getAppSettings() {
        return appSettings;
    }
    
    /**
     * 获取当前所有配置的快照（供 REST 接口返回 JSON）
     */
    public java.util.Map<String, Object> getConfigSnapshot() {
        java.util.Map<String, Object> snapshot = new java.util.LinkedHashMap<>();
        snapshot.put("configFullContent", configFullContent);
        snapshot.put("appDebugEnabled", appDebugEnabled);
        snapshot.put("scores", scores != null ? Arrays.toString(scores) : null);
        snapshot.put("userConfig", userConfig != null ? userConfig.toString() : null);
        snapshot.put("userConfigList", userConfigList != null ? userConfigList.toString() : null);
        snapshot.put("appSettings", appSettings != null ? appSettings.toString() : null);
        snapshot.put("name", name != null ? name : null);
        return snapshot;
    }
    
    // ================================================================
    // 辅助方法
    // ================================================================
    
    private static String indent(String text, String prefix) {
        if (text == null) {
            return prefix + "null";
        }
        return prefix + text.replace("\n", "\n" + prefix);
    }
}
