# Nacos 配置中心示例

> 配套文章：
>
> [《Nacos 2.x 源码深度解析 (一)：架构整体全貌 —— 核心模块划分与版本演进》](https://blog.csdn.net/Yuu_9977/article/details/161167369?spm=1001.2014.3001.5502)
>
> [《Nacos 2.x 源码深度解析 (二)：通信协议迭代 —— HTTP长轮询到gRPC演进》](https://blog.csdn.net/Yuu_9977/article/details/161397929?spm=1001.2014.3001.5502)
>
> [《Nacos 2.x 源码深度解析 (三)：配置中心客户端 —— 启动加载与自动装配》](https://blog.csdn.net/Yuu_9977/article/details/161373008?spm=1001.2014.3001.5502)
>
> [《Nacos 2.x 源码深度解析 (四)：配置中心服务端 —— 事件总线与数据持久化》](https://blog.csdn.net/Yuu_9977/article/details/161400534?spm=1001.2014.3001.5501)
>
> [《Nacos 2.x 源码深度解析 (五)：gRPC 推送链路 —— 配置变更下发与动态刷新》](https://blog.csdn.net/Yuu_9977/article/details/161401011?spm=1001.2014.3001.5501)
>
> [《Nacos 2.x 源码深度解析 (六)：三级缓存体系 —— 降级兜底与故障自愈机制》](https://blog.csdn.net/Yuu_9977/article/details/161401280?spm=1001.2014.3001.5502)

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
    │   │   ├── DynamicConfigProperties.java      # 动态配置属性（@ConfigurationProperties + @RefreshScope）
    │   │   └── NacosConfigAnnotationDemo.java    # @NacosConfig 字段注入演示（6 种类型）
    │   ├── controller/
    │   │   ├── AnnotationDemoController.java     # 注解示例 REST 验证接口（@NacosConfig + @NacosConfigListener）
    │   │   ├── ConfigDemoController.java         # 配置演示（@RefreshScope + @Value 注入）
    │   │   └── NacosApiController.java           # ConfigService API 直接调用演示
    │   ├── listener/
    │   │   ├── ConfigChangeMonitor.java          # 事件监听 + 定时配置打印
    │   │   └── NacosConfigListenerDemo.java      # @NacosConfigListener 方法监听演示（6 种参数类型）
    │   └── model/
    │       └── UserConfig.java                   # JSON 配置模型类
    │   ├── config/
    │   │   └── LocalConfigSnapshotDemo.java        # 三级缓存与本地快照演示（文章六）
    │   ├── controller/
    │   │   └── NacosApiAdvancedController.java      # 高级 API（三级缓存、gRPC 推送、CAS）
    │   └── listener/
    │       └── GrpcPushReceiveDemo.java             # gRPC 推送接收演示（文章五）
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
  GET  /config/info             - 查看当前配置（@RefreshScope）
  GET  /config/app-name         - 快速查看 appName
  GET  /annotation/info         - 查看 @NacosConfig 字段注入值
  GET  /annotation/listener     - 查看 @NacosConfigListener 变更记录
  GET  /annotation/compare      - 对比两种注解的当前状态
  GET  /nacos/fetch?dataId=xx   - 手动拉取配置
  POST /nacos/listen?dataId=xx  - 注册配置监听器
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

### 五、Nacos 声明式注解：@NacosConfig 与 @NacosConfigListener

> `@NacosConfig` 和 `@NacosConfigListener` 是 Spring Cloud Alibaba Nacos Config 提供的声明式注解，
> 基于**观察者模式**设计，实现配置的**字段级自动注入**与**方法级变更回调**，无需手动调用 `ConfigService` API，
> 也无需 `@RefreshScope`。

#### 5.0 前置准备：在 Nacos 控制台创建配置

在 Nacos 控制台 (`http://127.0.0.1:8848/nacos`) 创建以下配置（均使用 `DEFAULT_GROUP`）：

| Data ID | 格式 | 配置内容 |
|---|---|---|
| `app-settings.properties` | Properties | `app.name=nacos-demo`<br>`app.version=2.0.0`<br>`app.debug=true`<br>`app.max-connections=100` |
| `scores.json` | JSON | `[95, 88, 76, 92, 100]` |
| `user-config.json` | JSON | `{"username":"admin","password":"123456","age":25,"email":"admin@example.com","roles":["ROLE_ADMIN","ROLE_USER"],"metadata":{"department":"engineering","level":"senior"}}` |
| `user-list.json` | JSON | `[{"username":"admin","age":25},{"username":"user1","age":30}]` |

#### 5.1 @NacosConfig — 字段注入（6 种类型）

> 对应 `NacosConfigAnnotationDemo.java`

`@NacosConfig` 将 Nacos 中的配置数据直接注入到 Java 字段，支持**自动类型转换**和 **JSON 反序列化**。
配置变更时，底层 Processor 自动替换字段引用，不依赖 `@RefreshScope`。

```java
@Component
public class NacosConfigAnnotationDemo {

    // 类型一：注入完整配置文本（String）—— 适用场景：透传给第三方 / 自行解析
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    private String configFullContent;

    // 类型二：注入指定 key 的基础类型（boolean）—— 适用场景：开关类配置
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP",
            key = "app.debug", defaultValue = "false")
    private boolean appDebugEnabled;

    // 类型三：JSON 数组 → int[] —— 适用场景：批量数值配置
    @NacosConfig(dataId = "scores.json", group = "DEFAULT_GROUP")
    private int[] scores;

    // 类型四：JSON 对象 → 自定义 JavaBean —— 适用场景：强类型业务对象
    @NacosConfig(dataId = "user-config.json", group = "DEFAULT_GROUP")
    private UserConfig userConfig;

    // 类型五：JSON 数组 → List<UserConfig> —— 适用场景：多实体列表
    @NacosConfig(dataId = "user-list.json", group = "DEFAULT_GROUP")
    private List<UserConfig> userConfigList;

    // 类型六：Properties 对象 —— 适用场景：遍历所有配置项 / 动态查找 key
    @NacosConfig(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    private Properties appSettings = new Properties();
}
```

| 注入类型 | Java 字段类型 | dataId 格式 | 典型场景 |
|---|---|---|---|
| 完整文本 | `String` | 任意 | 透传原始内容 |
| 指定 key | `boolean` / `int` / `long` / `float` / `double` | `.properties` + `key` 属性 | 开关/阈值 |
| 基础类型数组 | `int[]` / `long[]` / `double[]` | `.json` | 批量数值 |
| 自定义 Bean | 自定义 POJO | `.json` | 强类型业务对象 |
| Bean 列表 | `List<Bean>` | `.json` | 白名单/多实体 |
| Properties | `Properties` | `.properties` / `.yml` | 批量遍历 |

**验证方法：**

```bash
# 查看 @NacosConfig 字段注入的当前值
curl http://localhost:8080/annotation/info

# 在 Nacos 控制台修改 user-config.json（如 age: 25 → 30）→ 发布
# 等待 gRPC 推送（3-10 秒），再次调用接口，确认 userConfig.age 已自动更新
curl http://localhost:8080/annotation/info
```

**调用链路：**

```
应用启动 → NacosConfigAnnotationProcessor 扫描 @NacosConfig 字段
  → ConfigService.getConfig(dataId, group)  拉取远程配置
  → 类型转换（String / JSON 反序列化 / Properties 解析）
  → ReflectionUtils.setField() 注入字段

配置变更：
  gRPC BiRequestStream 推送 → AbstractConfigChangeListener
  → 字段引用自动替换 → @PostConstruct 可选二次确认
```

---

#### 5.2 @NacosConfigListener — 方法监听（6 种参数类型）

> 对应 `NacosConfigListenerDemo.java`

`@NacosConfigListener` 实现**声明式事件监听**：配置变更时，框架自动调用标注的方法，参数自动反序列化，
是观察者模式的优雅实践。

```java
@Component
public class NacosConfigListenerDemo {

    // 类型一：完整配置文本变更（String 参数）—— initNotify=true 启动时立即回调
    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP",
            initNotify = true)
    public void onAppSettingsFullContentChanged(String content) { ... }

    // 类型二：指定 key 的基础类型值变更（int 参数）
    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP",
            key = "app.max-connections")
    public void onMaxConnectionsChanged(int maxConnections) { ... }

    // 类型三：JSON 数组 → int[] 变更
    @NacosConfigListener(dataId = "scores.json", group = "DEFAULT_GROUP")
    public void onScoresChanged(int[] scores) { ... }

    // 类型四：JSON 对象 → UserConfig 变更（强类型回调）
    @NacosConfigListener(dataId = "user-config.json", group = "DEFAULT_GROUP")
    public void onUserConfigChanged(UserConfig userConfig) { ... }

    // 类型五：JSON 数组 → List<UserConfig> 变更
    @NacosConfigListener(dataId = "user-list.json", group = "DEFAULT_GROUP")
    public void onUserListChanged(List<UserConfig> userList) { ... }

    // 类型六：Properties 对象变更
    @NacosConfigListener(dataId = "app-settings.properties", group = "DEFAULT_GROUP")
    public void onPropertiesChanged(Properties properties) { ... }
}
```

| 方法参数类型 | dataId 格式 | 反序列化策略 | 典型场景 |
|---|---|---|---|
| `String` | 任意 | 直接透传 | 审计日志 / 同步 |
| `int` / `long` / `boolean`... | `.properties` + `key` | 类型转换 | 阈值精确监听 |
| `int[]` / `long[]`... | `.json` | JSON 数组反序列化 | 分数线 / ID 集合 |
| 自定义 Bean | `.json` | Jackson/Gson 反序列化 | 业务对象变更 |
| `List<Bean>` | `.json` | 泛型列表反序列化 | 白名单变更 |
| `Properties` | `.properties` / `.yml` | Properties 解析 | 增量对比 |

**验证方法：**

```bash
# 查看监听器缓存的变更记录（changeCount + latestUserConfig）
curl http://localhost:8080/annotation/listener

# 在 Nacos 控制台修改 user-config.json → 发布 → 再次调用
curl http://localhost:8080/annotation/listener
# changeCount 递增，latestUserConfig 展示新值
```

**调用链路：**

```
Nacos 控制台修改配置 → 发布
  → RpcConfigChangeNotifier.configDataChanged()
  → gRPC BiRequestStream.onNext(ConfigChangeNotifyRequest)
  → ConfigRpcTransportClient.handleConfigChangeNotifyRequest()
  → executeConfigListen() → MD5 校验 → 拉取新配置
  → NacosConfigListenerWrapper.receiveConfigInfo(newContent)
    → 根据方法参数类型选择反序列化器
    → Method.invoke(bean, convertedArgs)
    → @NacosConfigListener 方法被调用
```

---

#### 5.3 两种注解对比

```bash
# 一站式对比 @NacosConfig 和 @NacosConfigListener 的当前状态
curl http://localhost:8080/annotation/compare
```

| 对比维度 | @NacosConfig | @NacosConfigListener |
|---|---|---|
| 作用目标 | 字段 | 方法 |
| 注入/激活方式 | 字段引用替换 | 方法回调传参 |
| 类型支持 | JSON→Bean/List/Map + 基础类型 | 同左 + 基础类型 |
| 刷新机制 | 内置（不需要 @RefreshScope） | gRPC 推送 → 方法回调 |
| 框架依赖 | Spring Cloud Alibaba | Spring Cloud Alibaba |
| 所在 jar | `spring-alibaba-nacos-config` | `spring-alibaba-nacos-config` |
| 典型场景 | 复杂对象配置注入 | 审计/同步/告警回调 |

**设计模式：观察者模式**

```
Nacos Server（Subject / 主题）
  │  user-config.json / scores.json / app-settings.properties
  │
  │ gRPC BiRequestStream（双向长连接）
  │ RpcConfigChangeNotifier.configDataChanged()
  ▼
NacosConfigAnnotationProcessor（Observer Manager / 观察者管理器）
  │
  ├─ @NacosConfig 字段 ──── Observer（字段引用替换）
  └─ @NacosConfigListener 方法 ──── Observer（方法回调）
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
| 声明式注解 — 字段注入 | `@NacosConfig` → NacosConfigAnnotationProcessor | 6 种类型字段注入 | `NacosConfigAnnotationDemo.java` |
| 声明式注解 — 方法监听 | `@NacosConfigListener` → NacosConfigListenerWrapper | 6 种参数类型回调 | `NacosConfigListenerDemo.java` |
| 声明式注解 — REST 验证 | GET `/annotation/info` / `/listener` / `/compare` | 注解示例接口 | `AnnotationDemoController.java` |
| (四) 事件总线与持久化 | `ConfigOperationService.publishConfig()` | `/nacos/publish-advanced` | `NacosApiAdvancedController.java` |
| (五) gRPC 推送链路 | `RpcConfigChangeNotifier.configDataChanged()` | 注册监听器 | `GrpcPushReceiveDemo.java` |
| (五) 推送接收链路 | `GrpcClient.bindRequestStream()` → `handleConfigChangeNotifyRequest()` | `/nacos/push-stats` | `GrpcPushReceiveDemo.java` |
| (六) 三级缓存策略 | `LocalConfigInfoProcessor.saveSnapshot()` / `getFailover()` | `/nacos/cascade-fetch` | `LocalConfigSnapshotDemo.java`, `NacosApiAdvancedController.java` |

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
