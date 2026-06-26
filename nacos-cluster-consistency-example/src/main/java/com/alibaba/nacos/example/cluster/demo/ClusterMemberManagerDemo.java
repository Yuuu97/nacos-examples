package com.alibaba.nacos.example.cluster.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.ListView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 集群成员管理与节点感知演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (十二)：集群基础交互 —— 节点感知与基础数据同步》
 *
 * 演示 Nacos 集群成员管理的核心机制：
 * - 节点发现（MemberLookup 策略：FileConfigLookup → AddressServerLookup → StandaloneLookup）
 * - 节点状态管理（ServerMemberManager：UP / SUSPICIOUS / DOWN）
 * - 节点信息上报（MemberInfoReportTask：游标轮询，gRPC 优先、HTTP 降级）
 * - 责任节点判定（DistroMapper.responsible()：distroHash % servers.size()）
 *
 * 核心源码：
 *   ServerMemberManager.init()
 *     → InetUtils.getSelfIP() + ":" + port 构造 self 对象
 *     → initAndStartLookup() 启动节点发现
 *     → memberChange() 变更处理
 *   MemberUtil.onSuccess()/onFail()
 *     → 成功: UP + 重置失败计数
 *     → 失败: SUSPICIOUS → 累计 3 次 → DOWN
 *   DistroMapper.responsible(tag)
 *     → distroHash(tag) % servers.size()
 *
 * @author qinyu
 */
@Component
public class ClusterMemberManagerDemo {

    private static final Logger log = LoggerFactory.getLogger(ClusterMemberManagerDemo.class);

    @Value("${spring.cloud.nacos.discovery.server-addr:127.0.0.1:8848}")
    private String serverAddr;

    private NamingService namingService;

    /**
     * 模拟的集群成员列表
     */
    private final List<ClusterMember> memberList = new ArrayList<>();

    /**
     * 节点状态变更日志
     */
    private final List<StateChangeEvent> stateChangeLog = Collections.synchronizedList(new ArrayList<>());

    @PostConstruct
    public void init() {
        log.info("====== ClusterMemberManagerDemo 初始化 ======");
        try {
            Properties props = new Properties();
            props.setProperty("serverAddr", serverAddr);
            namingService = NamingFactory.createNamingService(props);

            // 解析配置中的集群地址
            String[] addresses = serverAddr.split(",");
            int id = 1;
            for (String addr : addresses) {
                addr = addr.trim();
                String[] parts = addr.split(":");
                String ip = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8848;
                ClusterMember member = new ClusterMember(id++, ip, port);
                member.state = "UP";
                memberList.add(member);
            }

            log.info("集群成员初始化完成，共 {} 个节点", memberList.size());
            for (ClusterMember member : memberList) {
                log.info("  [{}] {}:{}  state={}", member.id, member.ip, member.port, member.state);
            }

            log.info("集群节点发现机制对照:");
            log.info("  LookupFactory.createLookUp() → chooseLookup()");
            log.info("    ① 显式配置 nacos.core.member.lookup.type");
            log.info("    ② cluster.conf 文件或 member-list 非空");
            log.info("    ③ 回退 ADDRESS_SERVER");
            log.info("");
            log.info("节点状态管理:");
            log.info("  MemberUtil.onSuccess() → 强制 UP，重置失败计数");
            log.info("  MemberUtil.onFail()   → SUSPICIOUS，累计 3 次 → DOWN");
            log.info("============================================");

        } catch (NacosException e) {
            log.error("集群初始化失败: {}", e.getMessage());
        }
    }

    /**
     * 获取集群成员列表
     */
    public List<Map<String, Object>> getMembers() {
        List<Map<String, Object>> members = new ArrayList<>();
        for (ClusterMember member : memberList) {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("id", member.id);
            info.put("ip", member.ip);
            info.put("port", member.port);
            info.put("address", member.getAddress());
            info.put("state", member.state);
            info.put("failCount", member.failCount);
            info.put("isSelf", member.isSelf);
            info.put("isResponsible", member.isResponsible);
            members.add(info);
        }
        return members;
    }

    /**
     * 模拟成员状态变更
     */
    public Map<String, Object> simulateStateChange(int memberId, String newState) {
        ClusterMember member = findMember(memberId);
        if (member == null) {
            return Map.of("success", false, "error", "节点不存在");
        }

        String oldState = member.state;
        member.state = newState;
        if ("DOWN".equals(newState)) {
            member.failCount = 3;
        } else if ("UP".equals(newState)) {
            member.failCount = 0;
        }

        stateChangeLog.add(new StateChangeEvent(member.getAddress(), oldState, newState));

        log.info("节点状态变更: {}  {} → {}", member.getAddress(), oldState, newState);
        log.info("状态管理流程对照:");
        log.info("  MemberUtil.onFail() → {}", member.getAddress());
        log.info("    ① 设置 state = SUSPICIOUS");
        log.info("    ② failCount += 1");
        log.info("    ③ failCount >= 3 或 Connection refused → DOWN");
        log.info("  MemberUtil.onSuccess() → {}", member.getAddress());
        log.info("    ① 设置 state = UP");
        log.info("    ② 重置 failCount = 0");

        return Map.of(
                "success", true,
                "address", member.getAddress(),
                "oldState", oldState,
                "newState", newState
        );
    }

    /**
     * 模拟 Distro 责任节点判定
     */
    public Map<String, Object> simulateDistroMapping(String serviceName) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("serviceName", serviceName);

        // 模拟 DistroMapper.mapSrv() 逻辑
        // distroHash(tag) % servers.size()
        List<ClusterMember> healthyMembers = new ArrayList<>();
        for (ClusterMember m : memberList) {
            if ("UP".equals(m.state) || "SUSPICIOUS".equals(m.state)) {
                healthyMembers.add(m);
            }
        }

        if (healthyMembers.isEmpty()) {
            result.put("error", "无健康节点");
            return result;
        }

        int hash = serviceName.hashCode() & 0x7fffffff;
        int index = hash % healthyMembers.size();
        ClusterMember responsible = healthyMembers.get(index);

        result.put("hash", hash);
        result.put("totalMembers", memberList.size());
        result.put("healthyMembers", healthyMembers.size());
        result.put("responsibleMember", responsible.getAddress());

        log.info("Distro 责任节点判定: service={}", serviceName);
        log.info("  distroHash({}) = {}", serviceName, hash);
        log.info("  healthyList.size() = {}", healthyMembers.size());
        log.info("  mapSrv() → {} (index={})", responsible.getAddress(), index);
        log.info("  responsible() = {}",
                index == 0 ? "true (首个健康节点)" : "false (非本节点)");

        return result;
    }

    private ClusterMember findMember(int id) {
        for (ClusterMember m : memberList) {
            if (m.id == id) return m;
        }
        return null;
    }

    /**
     * 获取状态变更日志
     */
    public List<StateChangeEvent> getStateChangeLog() {
        return new ArrayList<>(stateChangeLog);
    }

    /**
     * 集群成员模型
     */
    static class ClusterMember {
        final int id;
        final String ip;
        final int port;
        volatile String state = "UP";
        volatile int failCount;
        volatile boolean isSelf;
        volatile boolean isResponsible;

        ClusterMember(int id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }

        String getAddress() {
            return ip + ":" + port;
        }
    }

    /**
     * 状态变更事件
     */
    static class StateChangeEvent {
        final String address;
        final String fromState;
        final String toState;
        final long timestamp;

        StateChangeEvent(String address, String fromState, String toState) {
            this.address = address;
            this.fromState = fromState;
            this.toState = toState;
            this.timestamp = System.currentTimeMillis();
        }
    }

    @PreDestroy
    public void destroy() {
        if (namingService != null) {
            try {
                namingService.shutDown();
            } catch (NacosException e) {
                log.warn("关闭 NamingService 异常", e);
            }
        }
    }
}
