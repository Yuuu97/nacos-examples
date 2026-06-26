package com.alibaba.nacos.example.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 集群一致性示例 —— 主启动类
 *
 * 对应文章：
 *   《Nacos 2.x 源码深度解析 (十二)：集群基础交互 —— 节点感知与基础数据同步》
 *   《Nacos 2.x 源码深度解析 (十三)：Distro 协议 ——AP 模式异步数据同步原理》
 *   《Nacos 2.x 源码深度解析 (十四)：Distro 容错处理 —— 数据校验与冲突修复》
 *   《Nacos 2.x 源码深度解析 (十五)：JRaft 架构 ——CP 集群 Leader 选举机制》
 *   《Nacos 2.x 源码深度解析 (十六)：JRaft 日志复制 —— 同步规则与快照管理》
 *
 * 本模块演示 Nacos 集群一致性核心机制，包括：
 * - 节点感知与基础交互（ServerMemberManager + MemberLookup）
 * - Distro 协议 AP 模式数据同步（双层任务引擎 + 增量同步）
 * - Distro 容错处理（数据校验 + 冲突修复 + 补偿同步）
 * - JRaft CP 模式 Leader 选举（NacosStateMachine + 投票机制）
 * - JRaft 日志复制与快照（onApply + onSnapshotSave/onSnapshotLoad）
 *
 * @author qinyu
 */
@SpringBootApplication
@EnableScheduling
public class NacosClusterConsistencyExampleApplication {

    private static final Logger log = LoggerFactory.getLogger(NacosClusterConsistencyExampleApplication.class);

    public static void main(String[] args) {
        log.info("=====================================================");
        log.info("  Nacos 集群一致性示例应用 启动中...");
        log.info("  文章: 第十二~十六篇 - 集群一致性篇");
        log.info("=====================================================");
        SpringApplication.run(NacosClusterConsistencyExampleApplication.class, args);
        log.info("=====================================================");
        log.info("  应用启动完成！");
        log.info("  测试接口：");
        log.info("    GET  /cluster/members             - 查看集群成员列表");
        log.info("    GET  /cluster/distro/status        - Distro 数据同步状态");
        log.info("    POST /cluster/distro/sync          - 模拟 Distro 数据同步");
        log.info("    POST /cluster/distro/verify        - 模拟 Distro 数据校验");
        log.info("    GET  /cluster/jraft/status         - JRaft 集群状态");
        log.info("    POST /cluster/jraft/elect          - 模拟 Leader 选举");
        log.info("    POST /cluster/jraft/replicate      - 模拟日志复制");
        log.info("=====================================================");
    }
}
