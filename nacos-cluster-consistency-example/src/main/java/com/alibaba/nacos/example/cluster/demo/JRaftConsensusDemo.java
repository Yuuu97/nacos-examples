package com.alibaba.nacos.example.cluster.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JRaft 共识算法演示
 *
 * 对应文章：
 *   (十五)：JRaft 架构 ——CP 集群 Leader 选举机制
 *   (十六)：JRaft 日志复制 —— 同步规则与快照管理
 *
 * 演示 JRaft 共识算法的核心机制：
 *
 * 1. Leader 选举
 *    - electionTimeout 默认 5000ms
 *    - 心跳间隔 = electionTimeout / 10 = 500ms
 *    - 随机偏移 [5000ms, 6000ms] 防止选举冲突
 *    - NacosStateMachine.onLeaderStart() / onLeaderStop()
 *
 * 2. 日志复制
 *    - onApply(Iterator) 双来源分流: Leader 直接获取 / Follower 反序列化
 *    - NacosClosure 双层封装: closure.getMessage() → processor.onApply()
 *    - 异常回滚: iter.setErrorAndRollback()
 *
 * 3. 快照管理
 *    - onSnapshotSave / onSnapshotLoad
 *    - JSnapshotOperation 三层适配
 *    - 快照间隔 1800s (30分钟)
 *
 * @author qinyu
 */
@Component
public class JRaftConsensusDemo {

    private static final Logger log = LoggerFactory.getLogger(JRaftConsensusDemo.class);

    /**
     * Raft 节点
     */
    private final List<RaftNode> nodes = new ArrayList<>();

    /**
     * 当前 Leader
     */
    private volatile RaftNode currentLeader;

    /**
     * 任期
     */
    private final AtomicLong currentTerm = new AtomicLong(1);

    /**
     * 提交的日志条目
     */
    private final List<LogEntry> committedLog = Collections.synchronizedList(new ArrayList<>());

    /**
     * 选举计数
     */
    private final AtomicInteger electionCount = new AtomicInteger(0);

    /**
     * 日志复制计数
     */
    private final AtomicInteger replicateCount = new AtomicInteger(0);

    @PostConstruct
    public void init() {
        log.info("====== JRaftConsensusDemo 初始化 ======");

        // 创建 3 节点 Raft 集群
        for (int i = 1; i <= 3; i++) {
            RaftNode node = new RaftNode(i, "node-" + i + ":8087");
            nodes.add(node);
        }

        log.info("JRaft 集群初始化完成，共 {} 个节点", nodes.size());
        log.info("  electionTimeout = 5000ms (默认)");
        log.info("  心跳间隔 = 500ms (electionTimeout / 10)");
        log.info("  随机偏移 = [5000ms, 6000ms]");
        log.info("=========================================");
    }

    /**
     * 模拟 Leader 选举
     *
     * 对应 JRaft 选举流程:
     *   NacosStateMachine.onLeaderStart(term)
     *     → isLeader.set(true)
     *     → 记录 leaderIp
     *     → 发布 RaftEvent(groupId, leader, term, raftClusterInfo)
     *   NacosStateMachine.onLeaderStop(status)
     *     → isLeader.set(false)
     *
     *   JRaftServer.createMultiRaftGroup() 中的选举配置:
     *     nodeOptions.setSharedElectionTimer(true)
     *     nodeOptions.setSharedVoteTimer(true)
     *     nodeOptions.setSharedStepDownTimer(true)
     *     electionTimeout = Math.max(configVal, DEFAULT_ELECTION_TIMEOUT(5000))
     */
    public synchronized Map<String, Object> simulateElection() {
        int electionNo = electionCount.incrementAndGet();
        long term = currentTerm.incrementAndGet();

        // 随机选择一个节点作为 Leader
        int leaderIndex = ThreadLocalRandom.current().nextInt(nodes.size());
        RaftNode newLeader = nodes.get(leaderIndex);

        // 新旧 Leader 切换
        RaftNode oldLeader = currentLeader;
        if (oldLeader != null) {
            oldLeader.isLeader = false;
            oldLeader.term = term;
            log.info("[选举 #{}] 旧 Leader {} 退位 (term={})", electionNo, oldLeader.id, term);
            log.info("  → NacosStateMachine.onLeaderStop() → isLeader.set(false)");
        }

        newLeader.isLeader = true;
        newLeader.term = term;
        currentLeader = newLeader;

        log.info("[选举 #{}] 新 Leader 产生: {} (term={})", electionNo, newLeader.id, term);
        log.info("选举流程追踪:");
        log.info("  → 当前节点 electionTimeout 到期 (默认 5000ms)");
        log.info("    → 状态变为 Candidate");
        log.info("    → 发起 PreVote → RequestVote");
        log.info("  → 收到多数派投票 (quorum = {})", nodes.size() / 2 + 1);
        log.info("  → 成为 Leader (term={})", term);
        log.info("  → NacosStateMachine.onLeaderStart(term={})", term);
        log.info("    → isLeader.set(true)");
        log.info("    → 记录 leaderIp = {}", newLeader.id);
        log.info("    → 发布 RaftEvent(groupId, leader, term, clusterInfo)");
        log.info("  → 开始发送心跳 (interval = 500ms)");

        // 其他节点模拟
        for (RaftNode node : nodes) {
            if (node != newLeader) {
                log.info("    Follower {} 收到心跳 → 重置 electionTimeout", node.id);
            }
        }

        return Map.of(
                "electionNo", electionNo,
                "term", term,
                "oldLeader", oldLeader != null ? oldLeader.id : null,
                "newLeader", newLeader.id,
                "nodes", nodes.stream().map(n -> Map.of(
                        "id", n.id,
                        "address", n.address,
                        "isLeader", n.isLeader,
                        "term", n.term
                )).toList()
        );
    }

    /**
     * 模拟日志复制
     *
     * 对应 JRaft 日志复制流程:
     *   JRaftProtocol.writeAsync(WriteRequest)
     *     → JRaftServer.commit()
     *       → Leader: applyOperation(node, data, closure)
     *         → NacosClosure(message, closure)
     *         → node.apply(task) 提交 Raft 日志
     *       → Follower: invokeToLeader()
     *         → cliClientService.getRpcClient().invokeAsync()
     *           → 转发给 Leader
     *   NacosStateMachine.onApply(Iterator)
     *     → iter.done() != null → Leader 路径: closure.getMessage()
     *     → iter.done() == null → Follower 路径: ProtoMessageUtil.parse()
     *     → Follower 跳过 ReadRequest
     *     → postProcessor() → closure.run(status) → future.complete(response)
     *     → 异常: iter.setErrorAndRollback()
     */
    public Map<String, Object> simulateReplicate(String data) {
        if (currentLeader == null) {
            return Map.of("success", false, "error", "集群无 Leader，请先执行选举");
        }

        int replicateNo = replicateCount.incrementAndGet();
        long index = committedLog.size() + 1;

        // 模拟日志条目
        LogEntry entry = new LogEntry(index, currentTerm.get(), data, currentLeader.id);
        committedLog.add(entry);

        log.info("[日志复制 #{}] 提交日志: index={}, term={}, data={}",
                replicateNo, index, currentTerm.get(), data);
        log.info("日志复制流程追踪:");
        log.info("  → JRaftProtocol.writeAsync(WriteRequest)");
        log.info("    → JRaftServer.commit(group, data, future)");
        log.info("    → 当前节点是 Leader: Leader 路径");
        log.info("      → applyOperation(node, data, closure)");
        log.info("        → new NacosClosure(message, closure)");
        log.info("        → node.apply(task) 提交 Raft 日志");
        log.info("  → NacosStateMachine.onApply(Iterator)");
        log.info("    → iter.done() != null: Leader 路径");
        log.info("      → closure.getMessage() 获取原始 Message");
        log.info("      → processor.onApply(WriteRequest)");
        log.info("    → iter.done() == null: Follower 路径");
        log.info("      → ProtoMessageUtil.parse() 反序列化");
        log.info("      → Follower 跳过 ReadRequest (continue)");
        log.info("    → postProcessor(response, closure)");
        log.info("    → closure.run(status) → future.complete(response)");
        log.info("    → 异常 → iter.setErrorAndRollback()");

        // 模拟 Follower 复制
        for (RaftNode node : nodes) {
            if (node != currentLeader) {
                log.info("    Follower {} 已复制日志 index={}", node.id, index);
            }
        }

        return Map.of(
                "success", true,
                "replicateNo", replicateNo,
                "index", index,
                "term", currentTerm.get(),
                "leader", currentLeader.id,
                "data", data,
                "totalLogs", committedLog.size()
        );
    }

    /**
     * 获取 JRaft 集群状态
     */
    public Map<String, Object> getClusterStatus() {
        List<Map<String, Object>> nodeInfos = new ArrayList<>();
        for (RaftNode node : nodes) {
            nodeInfos.add(Map.of(
                    "id", node.id,
                    "address", node.address,
                    "isLeader", node.isLeader,
                    "term", node.term
            ));
        }

        List<Map<String, Object>> logEntries = new ArrayList<>();
        for (LogEntry entry : committedLog) {
            logEntries.add(Map.of(
                    "index", entry.index,
                    "term", entry.term,
                    "data", entry.data,
                    "leaderId", entry.leaderId
            ));
        }

        return Map.of(
                "leader", currentLeader != null ? currentLeader.id : null,
                "currentTerm", currentTerm.get(),
                "electionCount", electionCount.get(),
                "replicateCount", replicateCount.get(),
                "nodes", nodeInfos,
                "committedLogs", logEntries
        );
    }

    /**
     * Raft 节点
     */
    static class RaftNode {
        final int id;
        final String address;
        volatile boolean isLeader;
        volatile long term;

        RaftNode(int id, String address) {
            this.id = id;
            this.address = address;
        }
    }

    /**
     * 日志条目
     */
    static class LogEntry {
        final long index;
        final long term;
        final String data;
        final int leaderId;

        LogEntry(long index, long term, String data, int leaderId) {
            this.index = index;
            this.term = term;
            this.data = data;
            this.leaderId = leaderId;
        }
    }
}
