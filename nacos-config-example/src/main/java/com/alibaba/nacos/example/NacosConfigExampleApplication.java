package com.alibaba.nacos.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Nacos 配置中心示例 —— 主启动类
 *
 * 启动流程（对应文章一、客户端启动章节）：
 * 1. Bootstrap 引导上下文：
 *    spring-cloud-starter-bootstrap 依赖触发 BootstrapApplicationListener，
 *    创建 BootstrapContext 父上下文，优先加载 bootstrap.yml 中的
 *    Nacos Server 连接参数（server-addr、namespace、认证信息）
 * 2. 自动装配入口：
 *    @SpringBootApplication 中的 @EnableAutoConfiguration 触发自动配置：
 *    - NacosConfigAutoConfiguration → 初始化 NacosConfigManager、
 *      NacosContextRefresher（监听器注册入口）
 *    - NacosConfigSpringCloudAutoConfiguration → 注册
 *      NacosConfigRefreshEventListener（Nacos 事件 → Spring Cloud 事件）
 * 3. 配置拉取：
 *    NacosConfigDataLocationResolver 解析 spring.config.import 中的 nacos: 前缀
 *    → NacosConfigDataLoader.doLoad() 通过 ConfigService.getConfig()
 *    拉取远程配置 → 封装为 NacosPropertySource → 注入 Environment
 * 4. 监听订阅：
 *    容器就绪后，NacosContextRefresher.registerNacosListenersForApplications()
 *    遍历所有 NacosPropertySource → registerNacosListener(…) 为每个配置
 *    通过 gRPC 双向流注册监听器 → 准备接收服务端推送
 *
 * 配置中心篇扩展覆盖（对应文章四~六）：
 * - 配置中心服务端：事件总线与数据持久化
 * - gRPC 推送链路：配置变更下发与动态刷新
 * - 三级缓存体系：Failover 容灾 → gRPC 远程 → Snapshot 本地快照降级兜底
 *
 * @EnableScheduling 的作用：
 * 启用 Spring 定时任务调度，配合 ConfigChangeMonitor 中的 @Scheduled 方法，
 * 定时打印当前配置值，直观展示配置变更前后的对比效果
 *
 * @author qinyu
 */
@SpringBootApplication
@EnableScheduling
public class NacosConfigExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(NacosConfigExampleApplication.class);

    public static void main(String[] args) {
        log.info("=====================================================");
        log.info("  Nacos 配置中心示例应用 启动中...");
        log.info("  文章: 第二篇 - Nacos 配置中心通信推送与动态刷新源码分析");
        log.info("=====================================================");
        SpringApplication.run(NacosConfigExampleApplication.class, args);
        log.info("=====================================================");
        log.info("  应用启动完成！");
        log.info("  测试接口：");
        log.info("    GET  /config/info           - 查看当前配置（@RefreshScope）");
        log.info("    GET  /config/app-name       - 快速查看 appName");
        log.info("    GET  /nacos/fetch?dataId=xx - 手动拉取配置");
        log.info("    POST /nacos/listen?dataId=xx - 注册配置监听器");
        log.info("    POST /nacos/publish?dataId=xx - 发布配置");
        log.info("    POST /nacos/publish-cas?dataId=xx - CAS 发布配置");
        log.info("    GET  /nacos/health          - 查看 Server 健康状态");
        log.info("    GET  /config/snapshot        - 查看本地快照状态");
        log.info("    POST /nacos/publish-advanced  - 高级配置发布（指定类型）");
        log.info("    DELETE /nacos/remove          - 删除配置");
        log.info("    GET  /nacos/server-status     - 查看 Server 健康状态");
        log.info("    GET  /config/cache-stats      - 缓存状态统计");
        log.info("=====================================================");
    }
}
