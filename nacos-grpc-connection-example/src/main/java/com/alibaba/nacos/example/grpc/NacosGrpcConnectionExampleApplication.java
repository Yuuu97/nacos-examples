package com.alibaba.nacos.example.grpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * gRPC 连接内核示例 —— 主启动类
 *
 * 对应文章：
 *   《Nacos 2.x 源码深度解析 (九)：双向流设计 —— 连接创建复用与销毁》
 *   《Nacos 2.x 源码深度解析 (十)：心跳保活策略 —— 断线检测与重连源码》
 *   《Nacos 2.x 源码深度解析 (十一)：RPC 请求调度 —— 收发模型与线程池处理》
 *
 * 本模块演示 Nacos gRPC 连接内核的核心机制，通过 Nacos Client API 间接展示：
 * - gRPC 双向流连接建立（connectToServer 流程）
 * - 心跳保活与断线检测（healthCheck + reconnect）
 * - RPC 请求调度模型（同步/异步/Future 三种模式）
 *
 * @author qinyu
 */
@SpringBootApplication
@EnableScheduling
public class NacosGrpcConnectionExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(NacosGrpcConnectionExampleApplication.class);

    public static void main(String[] args) {
        log.info("=====================================================");
        log.info("  Nacos gRPC 连接内核示例应用 启动中...");
        log.info("  文章: 第九~十一篇 - gRPC 连接内核篇");
        log.info("=====================================================");
        SpringApplication.run(NacosGrpcConnectionExampleApplication.class, args);
        log.info("=====================================================");
        log.info("  应用启动完成！");
        log.info("  测试接口：");
        log.info("    GET  /grpc/connection/status         - 查看 gRPC 连接状态");
        log.info("    GET  /grpc/connection/info           - 连接详细信息");
        log.info("    POST /grpc/connection/check          - 手动触发健康检查");
        log.info("    GET  /grpc/rpc/stats                 - RPC 请求统计");
        log.info("    POST /grpc/rpc/sync                  - 模拟同步 RPC 调用");
        log.info("    POST /grpc/rpc/async                 - 模拟异步 RPC 调用");
        log.info("    GET  /grpc/event-log                 - 查看连接事件日志");
        log.info("=====================================================");
    }
}
