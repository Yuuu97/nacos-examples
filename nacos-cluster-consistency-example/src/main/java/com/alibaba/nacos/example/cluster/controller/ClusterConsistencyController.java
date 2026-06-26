package com.alibaba.nacos.example.cluster.controller;

import com.alibaba.nacos.example.cluster.demo.ClusterMemberManagerDemo;
import com.alibaba.nacos.example.cluster.demo.DistroProtocolDemo;
import com.alibaba.nacos.example.cluster.demo.JRaftConsensusDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 集群一致性演示 Controller
 *
 * @author qinyu
 */
@RestController
@RequestMapping("/cluster")
public class ClusterConsistencyController {

    @Autowired
    private ClusterMemberManagerDemo clusterMemberManagerDemo;

    @Autowired
    private DistroProtocolDemo distroProtocolDemo;

    @Autowired
    private JRaftConsensusDemo jRaftConsensusDemo;

    /**
     * 查看集群成员列表
     */
    @GetMapping("/members")
    public Map<String, Object> members() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("members", clusterMemberManagerDemo.getMembers());
        return result;
    }

    /**
     * 模拟节点状态变更
     */
    @PostMapping("/members/state")
    public Map<String, Object> changeMemberState(
            @RequestParam int memberId,
            @RequestParam(defaultValue = "DOWN") String state) {
        return clusterMemberManagerDemo.simulateStateChange(memberId, state);
    }

    /**
     * Distro 责任节点判定
     */
    @GetMapping("/distro/responsible")
    public Map<String, Object> distroResponsible(
            @RequestParam(defaultValue = "example-service") String serviceName) {
        return clusterMemberManagerDemo.simulateDistroMapping(serviceName);
    }

    /**
     * Distro 数据同步状态
     */
    @GetMapping("/distro/status")
    public Map<String, Object> distroStatus() {
        return distroProtocolDemo.getSyncStatus();
    }

    /**
     * 模拟 Distro 数据同步
     */
    @PostMapping("/distro/sync")
    public Map<String, Object> distroSync(
            @RequestParam(defaultValue = "service-instance") String dataKey,
            @RequestParam(defaultValue = "{\"ip\":\"10.0.0.1\",\"port\":8848}") String content) {
        return distroProtocolDemo.simulateSync(dataKey, content);
    }

    /**
     * 模拟 Distro 数据校验
     */
    @PostMapping("/distro/verify")
    public Map<String, Object> distroVerify() {
        return distroProtocolDemo.simulateVerify();
    }

    /**
     * JRaft 集群状态
     */
    @GetMapping("/jraft/status")
    public Map<String, Object> jraftStatus() {
        return jRaftConsensusDemo.getClusterStatus();
    }

    /**
     * 模拟 Leader 选举
     */
    @PostMapping("/jraft/elect")
    public Map<String, Object> jraftElect() {
        return jRaftConsensusDemo.simulateElection();
    }

    /**
     * 模拟日志复制
     */
    @PostMapping("/jraft/replicate")
    public Map<String, Object> jraftReplicate(
            @RequestParam(defaultValue = "config-data-v1") String data) {
        return jRaftConsensusDemo.simulateReplicate(data);
    }

    /**
     * 集群状态变更日志
     */
    @GetMapping("/state-change-log")
    public Map<String, Object> stateChangeLog() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("events", clusterMemberManagerDemo.getStateChangeLog());
        return result;
    }
}
