package com.alibaba.nacos.example.naming;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Nacos NamingService API 示例 —— 主启动类
 *
 * 对应文章：
 *   《Nacos 2.x 源码深度解析 (七)：服务注册流程 —— 客户端上报与服务端存储》
 *   《Nacos 2.x 源码深度解析 (八)：服务订阅机制 —— 从首次订阅到gRPC双向流变更通知》
 *
 * 本模块演示 Nacos NamingService 核心 API 的调用方式，包括：
 * - 服务注册（临时实例 + 持久实例）
 * - 服务注销
 * - 服务订阅与取消订阅
 * - 服务实例查询
 * - 实例心跳模拟
 *
 * 核心源码对照：
 *   NacosNamingService.registerInstance()
 *     → NamingGrpcClientProxy.registerService()
 *       → registerServiceForEphemeral() / doRegisterServiceForPersistent()
 *     → InstanceRequestHandler.handle()  // 服务端接收
 *
 * @author qinyu
 */
@SpringBootApplication
@EnableScheduling
public class NacosNamingExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(NacosNamingExampleApplication.class);

    public static void main(String[] args) {
        log.info("=====================================================");
        log.info("  Nacos NamingService API 示例应用 启动中...");
        log.info("  文章: 第七、八篇 - 服务注册发现篇");
        log.info("=====================================================");
        SpringApplication.run(NacosNamingExampleApplication.class, args);
        log.info("=====================================================");
        log.info("  应用启动完成！");
        log.info("  测试接口：");
        log.info("    POST /naming/register           - 注册服务实例");
        log.info("    POST /naming/deregister          - 注销服务实例");
        log.info("    GET  /naming/instances           - 查询服务实例");
        log.info("    POST /naming/subscribe           - 订阅服务");
        log.info("    POST /naming/unsubscribe         - 取消订阅");
        log.info("    GET  /naming/subscribed-services - 已订阅服务列表");
        log.info("    GET  /naming/server-status       - 查看 Server 状态");
        log.info("=====================================================");
    }
}
