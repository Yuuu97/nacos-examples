# Nacos 集群一致性示例

> 配套文章：
>
> 

本模块演示 Nacos 集群一致性核心机制，涵盖 Distro AP 模式和 JRaft CP 模式。

## 项目结构

```
nacos-cluster-consistency-example/
├── pom.xml
└── src/main/java/com/alibaba/nacos/example/cluster/
    ├── NacosClusterConsistencyExampleApplication.java    # 启动类
    ├── demo/
    │   ├── ClusterMemberManagerDemo.java                 # 集群成员管理
    │   ├── DistroProtocolDemo.java                       # Distro 协议
    │   └── JRaftConsensusDemo.java                       # JRaft 共识
    └── controller/
        └── ClusterConsistencyController.java             # REST 接口
```

## 快速启动

```bash
cd nacos-cluster-consistency-example
mvn spring-boot:run
```

## 测试接口

### 查看集群成员
```bash
curl "http://localhost:8087/cluster/members"
```

### 模拟节点状态变更
```bash
curl -X POST "http://localhost:8087/cluster/members/state?memberId=1&state=DOWN"
```

### Distro 数据同步
```bash
curl -X POST "http://localhost:8087/cluster/distro/sync" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "dataKey=my-service&content={\"ip\":\"10.0.0.1\",\"port\":8848}"
```

### Distro 数据校验
```bash
curl -X POST "http://localhost:8087/cluster/distro/verify"
```

### JRaft Leader 选举
```bash
curl -X POST "http://localhost:8087/cluster/jraft/elect"
```

### JRaft 日志复制
```bash
curl -X POST "http://localhost:8087/cluster/jraft/replicate" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "data=my-config-data-v1"
```

## 核心链路

### 节点感知闭环
```
ServerMemberManager.init()
  → LookupFactory.createLookUp() → MemberLookup.start()
    → afterLookup() → memberChange()
      → MemberInfoReportTask (每 2 秒游标轮询)
        → MemberUtil.onSuccess/onFail (UP/SUSPICIOUS/DOWN)
      → MembersChangeEvent → 通知各组件
```

### Distro 数据同步闭环
```
ClientChangedEvent
  → DistroClientDataProcessor.onEvent()
    → DistroProtocol.sync() 广播
      → DistroDelayTask (1000ms 延迟合并)
        → DistroSyncChangeTask
          → DistroTransportAgent.syncData() gRPC 发送
    → 接收端: upgradeClient() 增量同步
      → 全量比对 → 增量更新 → 清理孤儿 → 更新 revision
```

### Distro 校验闭环
```
DistroVerifyTimedTask (每 5 秒)
  → getVerifyData() → DistroClientVerifyInfo(clientId, revision)
  → processVerifyData() → verifyClient()
    → revision == 0 (向后兼容) || 精确匹配
  → 失败 → ClientVerifyFailedEvent
    → syncToVerifyFailedServer() 零延迟补偿
```

### JRaft 日志复制闭环
```
JRaftProtocol.writeAsync()
  → JRaftServer.commit()
    → Leader: applyOperation() → node.apply(task)
      → NacosStateMachine.onApply(Iterator)
        → iter.done() != null: Leader (closure.getMessage())
        → iter.done() == null: Follower (ProtoMessageUtil.parse())
        → processor.onApply() → closure.run(status)
    → Follower: invokeToLeader() 转发
```

## 源码对照

| 文章章节 | 核心类/方法 | 示例代码对应 |
|---------|------------|------------|
| §12 节点发现 | `ServerMemberManager` / `MemberLookup` | `ClusterMemberManagerDemo.init()` |
| §12 成员状态 | `MemberUtil.onSuccess/onFail` | `simulateStateChange()` |
| §12 Distro 映射 | `DistroMapper.responsible()` | `simulateDistroMapping()` |
| §13 双层任务引擎 | `DistroDelayTaskExecuteEngine` / `DistroExecuteTaskExecuteEngine` | `simulateSync()` 流程追踪 |
| §13 增量同步 | `upgradeClient()` 三步闭环 | 同步链路注释 |
| §14 数据校验 | `DistroVerifyTimedTask.run()` | `simulateVerify()` |
| §14 补偿同步 | `ClientVerifyFailedEvent` → `syncToVerifyFailedServer()` | 校验失败注释 |
| §15 Leader 选举 | `NacosStateMachine.onLeaderStart/onLeaderStop` | `simulateElection()` |
| §16 日志复制 | `NacosStateMachine.onApply()` | `simulateReplicate()` |
| §16 快照管理 | `onSnapshotSave/onSnapshotLoad` + `JSnapshotOperation` | 快照注释 |