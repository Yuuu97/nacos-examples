package com.alibaba.nacos.example.listener;

import com.alibaba.nacos.example.config.DynamicConfigProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.scope.refresh.RefreshScopeRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 配置变更监控组件 — 演示配置热刷新的完整事件链路
 *
 * <p>对应文章核心机制：</p>
 * <ul>
 *   <li><b>事件监听（三、客户端接收）：</b>
 *       通过 Spring @EventListener 监听 RefreshEvent，
 *       验证 NacosConfigRefreshEventListener 转换事件后触发的刷新流程</li>
 *   <li><b>定时验证（对应文章 SocketRefreshRunner）：</b>
 *       通过 @Scheduled 定时打印当前配置值，直观展示配置变更前后对比</li>
 *   <li><b>@RefreshScope 效果监控：</b>
 *       对比同一配置在 @RefreshScope Bean 和注入时机的差异</li>
 * </ul>
 *
 * <p><b>完整事件链路：</b></p>
 * <pre>
 *   Nacos Server 控制台修改配置
 *     → RpcConfigChangeNotifier 通过 gRPC 双向流推送轻量通知 (dataId+group+tenant)
 *     → GrpcClient.bindRequestStream().onNext() 接收推送
 *     → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
 *     → 标记 consistentWithServer=false → notifyListenConfig()
 *     → executeConfigListen() → checkListenCache()
 *       → ConfigBatchListenRequest (携带本地 MD5) → Server 比对
 *       → 返回变更列表 → refreshContentAndCheck()
 *     → NacosContextRefresher.innerReceive()
 *       → refreshCountIncrement()
 *       → NacosRefreshHistory.addRefreshRecord()
 *       → applicationContext.publishEvent(NacosConfigRefreshEvent)
 *     → NacosConfigRefreshEventListener.onApplicationEvent()
 *       → applicationContext.publishEvent(new RefreshEvent(...))
 *     → ContextRefresher.refresh()  // Spring Cloud 刷新机制
 *       → Environment 重新加载
 *       → RefreshScope.refreshAll()  // 销毁所有 @RefreshScope Bean
 *       → 下次访问时重建 Bean，获取最新配置值
 *   </pre>
 *
 * @author nacos-examples
 */
@Component
public class ConfigChangeMonitor {

    private static final Logger log = LoggerFactory.getLogger(ConfigChangeMonitor.class);

    /**
     * 注入 DynamicConfigProperties（@RefreshScope Bean）
     * 当 RefreshScopeRefreshedEvent 触发后，此 Bean 已被销毁并重建
     */
    @Autowired
    private DynamicConfigProperties config;

    @PostConstruct
    public void init() {
        log.info("====== ConfigChangeMonitor 初始化 ======");
        log.info("组件已就绪，开始监听配置变更事件");
        log.info("初始配置: name={}, version={}", config.getName(), config.getVersion());
        log.info("=======================================");
    }

    // ============================================================
    // 事件监听 — 对应文章 NacosConfigRefreshEventListener
    // ============================================================

    /**
     * 监听 Spring Cloud 标准 RefreshEvent
     *
     * <p>此事件由 NacosConfigRefreshEventListener 触发：</p>
     * <pre>
     *   NacosConfigRefreshEvent (Nacos 私有)
     *     → NacosConfigRefreshEventListener.onApplicationEvent()
     *     → applicationContext.publishEvent(new RefreshEvent(...))
     *   </pre>
     *
     * <p>RefreshEvent 触发后：
     * <ol>
     *   <li>ContextRefresher.refresh() 清除 Environment 中的旧属性</li>
     *   <li>重新加载所有 PropertySource</li>
     *   <li>RefreshScope.refreshAll() 销毁 @RefreshScope Bean 缓存</li>
     *   <li>下次请求时重建 Bean，获取新配置值</li>
     * </ol>
     */
    @EventListener
    public void onRefreshEvent(RefreshScopeRefreshedEvent event) {
        log.info("======================================================");
        log.info("[RefreshScopeRefreshedEvent] @RefreshScope Bean 已完成刷新!");
        log.info("  事件来源: {}", event.getSource());
        log.info("  触发时间: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        log.info("  说明: Nacos 配置变更 → gRPC 推送 → NacosContextRefresher 发布事件");
        log.info("        → NacosConfigRefreshEventListener 转换事件 → RefreshScope 刷新完成");
        log.info("======================================================");
    }

    // ============================================================
    // 定时打印 — 演示 config.getRefreshInterval() 动态调整轮询频率
    // ============================================================

    /**
     * 定时打印当前配置值
     *
     * <p>fixedDelayString 使用 SpEL 表达式从 DynamicConfigProperties 中动态读取间隔：
     * 修改 Nacos 上 dynamic-config.yml 中的 app.refresh-interval 后，
     * 下一次调度会自动使用新的间隔值</p>
     *
     * <p>此方法的定时轮询机制，与文章 ConfigRpcTransportClient.startInternal() 中
     * 的 listenExecutebell.poll(5L, TimeUnit.SECONDS) 类似，都是通过轮询
     * 来感知配置变更（只是实现层面不同：一个是阻塞队列驱动的事件循环，一个是定时调度）</p>
     */
    @Scheduled(fixedDelayString = "#{@dynamicConfigProperties.refreshInterval}")
    public void reportCurrentConfig() {
        log.info("======== 当前配置（定时检查） ========");
        log.info("  name:    {}", config.getName());
        log.info("  version: {}", config.getVersion());
        log.info("  refresh-interval: {} ms", config.getRefreshInterval());
        log.info("======================================");
    }
}
