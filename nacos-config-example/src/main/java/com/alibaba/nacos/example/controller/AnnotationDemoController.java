package com.alibaba.nacos.example.controller;

import com.alibaba.nacos.example.config.NacosConfigAnnotationDemo;
import com.alibaba.nacos.example.listener.NacosConfigListenerDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@link com.alibaba.cloud.nacos.annotation.NacosConfig @NacosConfig} 与
 * {@link com.alibaba.cloud.nacos.annotation.NacosConfigListener @NacosConfigListener}
 * 两种 Nacos 注解的 REST 验证接口
 *
 * 通过此接口可以对比两种注解的工作机制：
 * - @NacosConfig：配置注入（Push → 字段引用替换，支持 JSON 反序列化）
 * - @NacosConfigListener：配置监听（Push → 回调方法执行，类型安全参数）
 *
 * 验证流程：
 * 1. 启动前先在 Nacos 控制台创建所需的配置（dataId 见各 Demo 类注释）
 * 2. 启动应用 → 观察控制台日志，确认配置注入 + 监听器注册成功
 * 3. GET /annotation/info → 查看 @NacosConfig 注入的当前值
 * 4. GET /annotation/listener → 查看 @NacosConfigListener 缓存的变更记录
 * 5. GET /annotation/compare → 对比两种注解的工作状态
 *
 * @author qinyu
 */
@RestController
@RequestMapping("/annotation")
public class AnnotationDemoController {

    private static final Logger log = LoggerFactory.getLogger(AnnotationDemoController.class);

    // ============================================================
    // 注入两个 Demo Bean
    // ============================================================

    private final NacosConfigAnnotationDemo configDemo;
    private final NacosConfigListenerDemo listenerDemo;

    public AnnotationDemoController(NacosConfigAnnotationDemo configDemo,
                                    NacosConfigListenerDemo listenerDemo) {
        this.configDemo = configDemo;
        this.listenerDemo = listenerDemo;
        log.info("[AnnotationDemoController] 注入完成：NacosConfigAnnotationDemo + "
                + "NacosConfigListenerDemo");
    }

    // ============================================================
    // 接口一：查看 @NacosConfig 注入的当前值
    // ============================================================

    /**
     * 查看 @NacosConfig 注解注入的所有字段当前值。
     *
     * 调用链路：
     *   HTTP GET /annotation/info
     *     → configDemo.getConfigSnapshot()
     *       → 读取各 @NacosConfig 字段引用
     *         → 这些字段值由 NacosConfigAnnotationProcessor 在配置变更时自动替换对象引用
     *
     * 验证点：
     * 修改 Nacos 配置后再次调用，userConfig / scores 等字段应展示新值
     */
    @GetMapping("/info")
    public Map<String, Object> getAnnotationConfigInfo() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        result.put("description", "@NacosConfig 字段注入的当前生效值");
        result.put("data", configDemo.getConfigSnapshot());
        return result;
    }

    // ============================================================
    // 接口二：查看 @NacosConfigListener 监听的变更记录
    // ============================================================

    /**
     * 查看 @NacosConfigListener 回调方法缓存的变更记录。
     *
     * 调用链路：
     *   Nacos 控制台修改配置 → 发布
     *     → gRPC BiRequestStream.onNext(ConfigChangeNotifyRequest)
     *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
     *     → executeConfigListen() → MD5 校验 → 拉取新配置
     *     → NacosConfigListenerWrapper.receiveConfigInfo(newContent)
     *     → Method.invoke(bean, deserializedArgs)
     *     → @NacosConfigListener 方法被调用
     *       → 更新 listenerDemo 中的 volatile 缓存字段
     *
     *   HTTP GET /annotation/listener
     *     → listenerDemo.getListenerSnapshot()
     *       → 读取最新缓存的配置值和变更计数
     *
     * 验证点：
     * changeCount 应随 Nacos 配置修改递增，latestUserConfig 展示最新对象
     */
    @GetMapping("/listener")
    public Map<String, Object> getListenerInfo() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        result.put("description", "@NacosConfigListener 回调方法的变更记录");
        result.put("data", listenerDemo.getListenerSnapshot());
        return result;
    }
    
    // ============================================================
    // 接口三：对比两种注解的工作状态
    // ============================================================

    /**
     * 对比 @NacosConfig 和 @NacosConfigListener 两种注解的当前状态。
     *
     * 两注解设计差异对比：
     * +------------------+---------------------------+-------------------------------+
     * | 对比维度          | @NacosConfig              | @NacosConfigListener          |
     * +------------------+---------------------------+-------------------------------+
     * | 作用目标          | 字段                      | 方法                          |
     * | 注入/激活方式     | 字段引用替换               | 方法回调传参                   |
     * | 类型支持          | JSON→Bean/List/Map+基础类型 | 同左 + 基础类型                |
     * | 刷新机制          | 内置（不需要 @RefreshScope）| gRPC 推送 → 方法回调           |
     * | 框架依赖          | Spring Cloud Alibaba       | Spring Cloud Alibaba          |
     * | 所在 jar          | spring-alibaba-nacos-config| spring-alibaba-nacos-config   |
     * | 典型场景          | 复杂对象配置注入            | 审计/同步/告警回调              |
     * +------------------+---------------------------+-------------------------------+
     */
    @GetMapping("/compare")
    public Map<String, Object> compareTwoAnnotations() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        // @NacosConfig
        Map<String, Object> configSide = new LinkedHashMap<>();
        configSide.put("mechanism", "@NacosConfig 字段注入（支持 JSON→对象反序列化）");
        configSide.put("values", configDemo.getConfigSnapshot());

        // @NacosConfigListener
        Map<String, Object> listenerSide = new LinkedHashMap<>();
        listenerSide.put("mechanism", "@NacosConfigListener 方法回调（观察者模式）");
        listenerSide.put("changeCount", listenerDemo.getChangeCount());
        listenerSide.put("lastChangeTime",
                listenerDemo.getLastChangeTime() != null
                        ? listenerDemo.getLastChangeTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        : "无变更");
        listenerSide.put("latestUserConfig",
                listenerDemo.getLatestUserConfig() != null
                        ? listenerDemo.getLatestUserConfig().toString()
                        : "未触发");
        
        result.put("@NacosConfig", configSide);
        result.put("@NacosConfigListener", listenerSide);
        return result;
    }
}
