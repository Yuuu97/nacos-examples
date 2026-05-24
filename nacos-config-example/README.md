# Nacos 配置中心示例

> 配套文章：[《第二篇：Naocs配置中心源码深度解析——gRPC长连接推送与动态刷新全流程》](第二篇：Naocs配置中心——通信推送与动态刷新源码分析.md)

本项目通过 Spring Boot 3.2.5 + Spring Cloud 2023.0.4 + Nacos 2.x 构建，完整演示 Nacos 配置中心的三大核心机制：**配置获取**、**gRPC 通信推送**与**动态配置刷新**。

---

## 目录

- [项目结构](#项目结构)
- [前置准备](#前置准备)
- [快速启动](#快速启动)
- [功能演示](#功能演示)
  - [一、配置获取](#一配置获取)
  - [二、长轮询 / gRPC 推送监听](#二长轮询--grpc-推送监听)
  - [三、动态配置刷新](#三动态配置刷新)
  - [四、配置发布（CAS 乐观锁）](#四配置发布cas-乐观锁)
- [文章源码对照](#文章源码对照)
- [核心链路梳理](#核心链路梳理)

---

## 项目结构

```
nacos-config-example/
├── pom.xml                                      # Maven 依赖配置
└── src/main/
    ├── java/com/alibaba/nacos/example/
    │   ├── NacosConfigExampleApplication.java    # 启动类（@SpringBootApplication + @EnableScheduling）
    │   ├── config/
    │   │   └── DynamicConfigProperties.java      # 动态配置属性（@ConfigurationProperties + @RefreshScope）
    │   ├── controller/
    │   │   ├── ConfigDemoController.java         # 配置演示（@RefreshScope + @Value 注入）
    │   │   └── NacosApiController.java           # ConfigService API 直接调用演示
    │   └── listener/
    │       └── ConfigChangeMonitor.java          # 事件监听 + 定时配置打印
    └── resources/
        ├── bootstrap.yml                         # Bootstrap 上下文配置（Nacos 连接参数）
        └── application.yml                       # 应用配置（本地兜底 + 业务配置）
```

---

## 前置准备

### 1. 启动 Nacos Server

确保 Nacos 2.x 服务端已启动在 `127.0.0.1:8848`（默认配置），或者修改 `bootstrap.yml` 中的 `spring.cloud.nacos.config.server-addr` 为实际地址。

### 2. 在 Nacos 控制台创建配置

访问 Nacos 控制台 `http://127.0.0.1:8848/nacos`，创建以下配置：

| Data ID | Group | 配置格式 | 配置内容 |
|---|---|---|---|
| `application-dev.yml` | `DEFAULT_GROUP` | YAML | （环境配置，可为空文件） |
| `dynamic-config.yml` | `DEFAULT_GROUP` | YAML | `app.name=nacos-config-demo` 等业务配置 |

**dynamic-config.yml 示例内容：**

```yaml
app:
  name: nacos-config-demo
  version: 1.0.0
  refresh-interval: 5000
```

### 3. 环境要求

- JDK 17+
- Maven 3.6+
- Nacos Server 2.x（本文以 2.x gRPC 通信机制为例）

---

## 快速启动

```bash
# 在项目根目录（nacos-examples/）下执行
mvn clean package -pl nacos-config-example

# 启动应用
cd nacos-config-example
mvn spring-boot:run
```

启动成功后控制台输出：

```
====== ConfigDemoController 初始化 ======
appName (from @Value): nacos-config-demo
appVersion (from @Value): 1.0.0
==========================================

====== ConfigChangeMonitor 初始化 ======
组件已就绪，开始监听配置变更事件
==========================================

应用启动完成！
测试接口：
  GET  /config/info           - 查看当前配置（@RefreshScope）
  GET  /config/app-name       - 快速查看 appName
  GET  /nacos/fetch?dataId=xx - 手动拉取配置
  POST /nacos/listen?dataId=xx - 注册配置监听器
  POST /nacos/publish?dataId=xx - 发布配置
```

---

## 功能演示

### 一、配置获取

> 对应文章第一章："客户端启动：配置是如何'拉'到本地的"

**1.1 通过 @Value / @ConfigurationProperties 获取配置**

应用启动时，`NacosConfigDataLoader` 自动从 Nacos Server 拉取 `dynamic-config.yml` 并注入 `Environment`：

```bash
# 查看当前所有配置（@Value 和 @ConfigurationProperties 两种注入方式对比）
curl http://localhost:8080/config/info
```

返回示例：

```json
{
  "timestamp": "2025-05-25T02:00:00",
  "@Value注入（@RefreshScope）": {
    "appName": "nacos-config-demo",
    "appVersion": "1.0.0"
  },
  "@ConfigurationProperties绑定（@RefreshScope）": {
    "name": "nacos-config-demo",
    "version": "1.0.0",
    "refreshInterval": 5000
  }
}
```

**1.2 直接通过 ConfigService API 拉取**

对应文章 `NacosConfigDataLoader.load()` → `ConfigService.getConfig()` 的内部调用链路：

```bash
# 一次性拉取 dynamic-config.yml 配置（不注册监听器）
curl "http://localhost:8080/nacos/fetch?dataId=dynamic-config.yml&group=DEFAULT_GROUP"
```

**源码对照：**

```
ConfigService.getConfig(dataId, group, timeoutMs)
  → ClientWorker.getServerConfig()
    → ConfigRpcTransportClient.queryConfigInner()  // gRPC 查询
    → LocalConfigInfoProcessor.saveSnapshot()       // 写本地快照
    → 返回配置内容
```

---

### 二、长轮询 / gRPC 推送监听

> 对应文章第一章"监听器的注册入口：NacosContextRefresher"和第二章"gRPC 推送：RpcConfigChangeNotifier"

**2.1 注册监听器**

注册后，当 Nacos 控制台修改该配置时，客户端会通过 gRPC 双向流收到推送通知：

```bash
# 注册对 dynamic-config.yml 的配置变更监听
curl -X POST "http://localhost:8080/nacos/listen?dataId=dynamic-config.yml"
```

**2.2 触发变更推送**

在 Nacos 控制台修改 `dynamic-config.yml` 的内容（比如将 `app.version` 改为 `1.1.0`），观察应用控制台日志：

```
================================================
[监听器回调 #1] 配置已变更!
  dataId : dynamic-config.yml
  group  : DEFAULT_GROUP
  新内容长度: 61 字符
================================================

======================================================
[RefreshEvent] 收到配置刷新事件!
  事件来源: NacosContextRefresher
  事件描述: Refresh Nacos config
  触发时间: 2025-05-25T02:01:00
======================================================
```

**源码对照：**

```
Nacos Server 控制台发布配置
  → ConfigOperationService.publishConfig()
    → ConfigChangePublisher.notifyConfigChange(ConfigDataChangeEvent)
    → NotifyCenter 事件总线广播
      ├─ DumpService.handleConfigDataChange()        // 本地磁盘 + 内存转储
      ├─ AsyncNotifyService.handleConfigDataChange()  // 集群节点同步
      └─ RpcConfigChangeNotifier.onEvent()           // gRPC 推送客户端
           → rpcPushService.pushWithCallback()
           → gRPC BiRequestStream 双向流向客户端推送

  客户端:
    → GrpcClient.bindRequestStream().onNext()
    → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
      → consistentWithServer = false  // 标记缓存失效
      → notifyListenConfig()          // 唤醒后台主循环
    → executeConfigListen()
    → checkListenCache()              // 批量 MD5 校验
      → refreshContentAndCheck()     // 拉取新配置
      → listener.receiveConfigInfo(newContent)  // 回调监听器
```

**完整事件链路（客户端侧）：**

```bash
# 观察 ScheduledConfigReporter 每 5 秒打印的配置变化
# 修改 Nacos 上的 app.version → 定时打印自动显示新值
```

---

### 三、动态配置刷新

> 对应文章第三章："客户端接收：从推送通知到配置生效"

**3.1 验证 @RefreshScope 自动刷新**

```bash
# 修改前查看配置
curl http://localhost:8080/config/app-name
# {"appName":"nacos-config-demo","version":"1.0.0"}

# 在 Nacos 控制台将 dynamic-config.yml 的 app.version 改为 1.1.0

# 等待几秒后再次查看（已自动刷新）
curl http://localhost:8080/config/app-name
# {"appName":"nacos-config-demo","version":"1.1.0"}
```

**3.2 事件驱动刷新链路**

`ConfigChangeMonitor` 通过 `@EventListener` 监听 `RefreshEvent`，直观展示事件驱动刷新的完整流程：

```java
// 1. Nacos Server 推送 → 客户端接收 → 校验拉取
ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
  → executeConfigListen() → checkListenCache()

// 2. 拉取成功 → 回调 AbstractSharedListener.innerReceive()
NacosContextRefresher.innerReceive()
  → applicationContext.publishEvent(NacosConfigRefreshEvent)

// 3. 事件转换：Nacos 私有事件 → Spring Cloud 标准事件
NacosConfigRefreshEventListener.onApplicationEvent()
  → applicationContext.publishEvent(new RefreshEvent(...))

// 4. 刷新配置，销毁并重建 @RefreshScope Bean
ContextRefresher.refresh()
  → Environment 重新加载
  → RefreshScope.refreshAll() 销毁缓存
  → 下次请求重建 Bean，获取新配置值
```

---

### 四、配置发布（CAS 乐观锁）

> 对应文章第二章："配置变更的触发入口：ConfigOperationService"

**4.1 普通发布**

```bash
# 通过 API 发布/更新配置
curl -X POST "http://localhost:8080/nacos/publish?dataId=test-config.yml&type=yaml" \
  -H "Content-Type: text/plain" \
  -d 'app:
  name: test-app
  version: 0.1.0'
```

**4.2 CAS 乐观锁发布（防止并发覆盖）**

模拟多人编辑同一配置的场景：

```bash
# Step 1: 先获取当前配置
curl "http://localhost:8080/nacos/fetch?dataId=test-config.yml"

# Step 2: CAS 方式发布（服务端会比对 MD5 防止覆盖）
curl -X POST "http://localhost:8080/nacos/publish-cas?dataId=test-config.yml&type=yaml" \
  -H "Content-Type: text/plain" \
  -d 'app:
  name: test-app
  version: 0.2.0'
```

如果当前配置的 MD5 与上次获取的不一致（已被他人修改），会返回：
```
CAS 发布失败！MD5 不一致，配置已被他人修改，请重新获取后再试。
```

---

## 文章源码对照

| 文章章节 | 核心类/方法 | 示例代码对应 | 代码位置 |
|---|---|---|---|
| §1 客户端启动 - 自动配置入口 | `@SpringBootApplication` → `@EnableAutoConfiguration` | 启动类 | `NacosConfigExampleApplication.java` |
| §1 客户端启动 - Bootstrap 引导 | `BootstrapApplicationListener` | Bootstrap 配置 | `bootstrap.yml` |
| §1 客户端启动 - Nacos 接入核心 | `NacosConfigDataLocationResolver` + `NacosConfigDataLoader` | spring.config.import 配置 | `bootstrap.yml` |
| §1 客户端启动 - 远程配置加载 | `NacosConfigDataLoader.doLoad()` → `ConfigService.getConfig()` | `/nacos/fetch` 接口 | `NacosApiController.java` |
| §1 动态刷新起点 | `NacosConfigRefreshEventListener` | `@EventListener(RefreshEvent)` | `ConfigChangeMonitor.java` |
| §1 监听器注册入口 | `NacosContextRefresher.registerNacosListener()` | `/nacos/listen` 接口 | `NacosApiController.java` |
| §1 底层通信入口 | `ConfigService.addListener()` / `getConfigAndSignListener()` | `/nacos/listen-atomic` 接口 | `NacosApiController.java` |
| §2 配置变更触发入口 | `ConfigOperationService.publishConfig()` | `/nacos/publish` 接口 | `NacosApiController.java` |
| §2 gRPC 推送 | `RpcConfigChangeNotifier.configDataChanged()` | 监听器回调中的 gRPC 推送流程（注释） | `NacosApiController.java` |
| §3 推送接收 | `GrpcClient.bindRequestStream()` | 事件监听链路的注释说明 | `ConfigChangeMonitor.java` |
| §3 配置变更处理核心 | `ConfigRpcTransportClient.handleConfigChangeNotifyRequest()` | 监听器回调流程 | `NacosApiController.java` |
| §3 批量监听检查 | `checkListenCache()` / `executeConfigListen()` | 后台主循环注释说明 | `NacosApiController.java` |
| §3 Spring 刷新 | `@RefreshScope` → `ContextRefresher.refresh()` | `@RefreshScope` 注解 + RefreshEvent 监听 | `ConfigDemoController.java`, `ConfigChangeMonitor.java` |
| §4 缓存兜底 | `LocalConfigInfoProcessor.saveSnapshot()` / `getFailover()` | `/nacos/fetch` 中的三级读取策略注释 | `NacosApiController.java` |
| §4 三级读取策略 | failover → remote gRPC → snapshot | `/nacos/fetch` 接口注释 | `NacosApiController.java` |

---

## 核心链路梳理

### 启动配置拉取链路

```
Spring Boot @SpringBootApplication
  → @EnableAutoConfiguration
    → spring-cloud-starter-bootstrap (Marker 类检测)
    → BootstrapApplicationListener
      → BootstrapContext 父上下文创建
      → 加载 bootstrap.yml
        → spring.cloud.nacos.config.server-addr=127.0.0.1:8848
        → spring.config.import: nacos:application-dev.yml
    → NacosConfigDataLocationResolver 解析 nacos: 前缀
    → NacosConfigDataLoader.doLoad()
      → ConfigService.getConfig(dataId, group)
        → ClientWorker.getServerConfig()
          → ConfigRpcTransportClient.queryConfigInner()  // gRPC 查询
          → LocalConfigInfoProcessor.saveSnapshot()       // 磁盘快照
      → NacosPropertySource → PropertySource → Environment
      → NacosPropertySourceRepository.collectNacosPropertySource()
    → NacosContextRefresher.registerNacosListenersForApplications()
      → configService.addListener()  // gRPC 双向流订阅
```

### 运行时热更新链路

```
Nacos 控制台发布配置
  → ConfigOperationService.publishConfig()
    → configInfoPersistService.insertOrUpdate()  // MySQL 持久化
    → ConfigChangePublisher.notifyConfigChange(ConfigDataChangeEvent)
    → NotifyCenter 事件总线广播:
      ├─ [订阅者1] DumpService
      │    → DumpTaskMgr.addTask(DumpTask)
      │    → NacosDelayTaskExecuteEngine (单线程, 100ms 轮询)
      │    → DumpProcessor.process()
      │      → ConfigCacheService.dumpWithMd5()
      │        → ConfigDiskServiceFactory.saveToDisk()  // 磁盘转储
      │        → updateMd5()  // 内存缓存更新
      │        → NotifyCenter.publishEvent(LocalDataChangeEvent)
      ├─ [订阅者2] AsyncNotifyService
      │    → clusterRpcClientProxy.syncConfigChange()  // 集群同步
      │    → 指数退避重试 (500ms ~ 最大6次)
      └─ [订阅者3] RpcConfigChangeNotifier
           → configDataChanged()
           → push(RpcPushTask)  // 指数退避 (tryTimes*2秒)
             → rpcPushService.pushWithCallback()
             → gRPC BiRequestStream → 客户端

客户端 (ConfigRpcTransportClient):
  → GrpcClient.bindRequestStream().onNext(Payload)
  → handleConfigChangeNotifyRequest(ConfigChangeNotifyRequest)
    → consistentWithServer = false
    → notifyListenConfig() → listenExecutebell.offer()
  → startInternal() 主循环 poll() 唤醒
  → executeConfigListen()
    → checkListenCache()  // 批量 ConfigBatchListenRequest
      → Server ConfigQueryRequestHandler.handle()
        → ConfigCacheService.getContentCache() → 磁盘读取
        → MD5 比对 → 返回变更列表
      → refreshContentAndCheck()
        → queryConfigInner() → 拉取新内容
        → listener.receiveConfigInfo(newContent)  ← AbstractSharedListener
    → NacosContextRefresher.innerReceive()
      → NacosConfigRefreshEvent
      → NacosConfigRefreshEventListener
        → RefreshEvent
        → ContextRefresher.refresh()
          → Environment 重新加载
          → @RefreshScope Bean 销毁重建
    → [每3分钟] needAllSync=true → 全量兜底 MD5 校验
```

### 缓存兜底三级读取策略

```
ConfigService.getConfig(dataId, group)
  ├─ ① failover 容灾文件 (高优先级)
  │    → ${user.home}/nacos/config/.../data/config-data/{group}/{dataId}
  │    → 用户手动放入，Server 不可用时的兜底
  │    → 命中则直接返回
  │
  ├─ ② 远程 gRPC 查询 (正常运行)
  │    → ConfigRpcTransportClient.queryConfigInner()
  │    → 服务端 ConfigQueryRequestHandler.handle()
  │      → ConfigCacheService.getContentCache() (内存缓存优先)
  │      → ConfigDiskServiceFactory.getContent() (磁盘读取)
  │    → 成功 → LocalConfigInfoProcessor.saveSnapshot() 写本地快照
  │
  └─ ③ 本地快照兜底 (Server 不可用)
       → LocalConfigInfoProcessor.getSnapshot()
       → ${user.home}/nacos/config/.../snapshot/{group}/{dataId}
       → 最后一次成功拉取的缓存 → 没有则返回 null
```
