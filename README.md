# Nacos 2.x 源码深度解析 — 配套示例代码

`nacos-examples` 是《Nacos 2.x 源码深度解析》专栏的配套示例代码，覆盖四大篇章的核心机制演示。

## 项目模块

| 模块 | 端口 | 覆盖篇章 | 文章编号 |
|------|------|---------|---------|
| `nacos-config-example` | 8080 | **配置中心篇** — 客户端启动、事件总线、gRPC推送、三级缓存 | (三)~(六) |
| `nacos-discovery-example/nacos-naming-example` | 8085 | **注册发现篇** — NamingService API、服务注册/订阅/心跳 | (七)(八) |
| `nacos-discovery-example/nacos-openfeign-*` | 8081~8082 | **集成篇** — OpenFeign 服务发现调用 | (七)(八) |
| `nacos-discovery-example/nacos-dubbo-*` | 8083~8084 | **集成篇** — Dubbo 服务发现调用 | (七)(八) |
| `nacos-grpc-connection-example` | 8086 | **gRPC 连接内核篇** — 双向流、心跳保活、RPC 调度 | (九)~(十一) |
| `nacos-cluster-consistency-example` | 8087 | **集群一致性篇** — 节点感知、Distro 同步、JRaft 选举 | (十二)~(十六) |

## 配套文章

### 第一阶段：架构通信篇
- [《Nacos 2.x 源码深度解析 (一)：架构整体全貌 —— 核心模块划分与版本演进》](https://blog.csdn.net/Yuu_9977/article/details/161167369?spm=1001.2014.3001.5502)
- [《Nacos 2.x 源码深度解析 (二)：通信协议迭代 —— HTTP长轮询到gRPC演进》](https://blog.csdn.net/Yuu_9977/article/details/161397929?spm=1001.2014.3001.5502)

### 第二阶段：配置中心篇
- [《Nacos 2.x 源码深度解析 (三)：配置中心客户端 —— 启动加载与自动装配》](https://blog.csdn.net/Yuu_9977/article/details/161373008?spm=1001.2014.3001.5502)
- [《Nacos 2.x 源码深度解析 (四)：配置中心服务端 —— 事件总线与数据持久化》](https://blog.csdn.net/Yuu_9977/article/details/161400534?spm=1001.2014.3001.5501)
- [《Nacos 2.x 源码深度解析 (五)：gRPC 推送链路 —— 配置变更下发与动态刷新》](https://blog.csdn.net/Yuu_9977/article/details/161401011?spm=1001.2014.3001.5501)
- [《Nacos 2.x 源码深度解析 (六)：三级缓存体系 —— 降级兜底与故障自愈机制》](https://blog.csdn.net/Yuu_9977/article/details/161401280?spm=1001.2014.3001.5502)

### 第三阶段：服务注册发现篇
- [《Nacos 2.x 源码深度解析 (七)：服务注册流程 —— 客户端上报与服务端存储》](https://blog.csdn.net/Yuu_9977/article/details/161495802?spm=1001.2014.3001.5502)
- [《Nacos 2.x 源码深度解析 (八)：服务订阅机制 —— 从首次订阅到gRPC双向流变更通知》](https://blog.csdn.net/Yuu_9977/article/details/161665254?spm=1011.2415.3001.5331)

### 第四阶段：gRPC 连接内核篇
- [《Nacos 2.x 源码深度解析 (九)：双向流设计 —— 连接创建复用与销毁》](https://blog.csdn.net/Yuu_9977/article/details/161889667?spm=1001.2014.3001.5501)
- [《Nacos 2.x 源码深度解析 (十)：心跳保活策略 —— 断线检测与重连源码》](https://blog.csdn.net/Yuu_9977/article/details/162269136?spm=1011.2415.3001.5331)
- [《Nacos 2.x 源码深度解析 (十一)：RPC 请求调度 —— 收发模型与线程池处理》](https://blog.csdn.net/Yuu_9977/article/details/162279139?spm=1011.2415.3001.5331)

### 第五阶段：集群一致性篇


## 前置准备

### 1. 启动 Nacos Server

确保 Nacos 2.x 服务端已启动在 `127.0.0.1:8848`。可以使用单机模式启动：

```bash
sh startup.sh -m standalone
```

### 2. 环境要求

- JDK 17+
- Maven 3.6+
- Nacos Server 2.x

## 构建与运行

```bash
# 全量构建
mvn clean compile -DskipTests

# 启动配置中心示例
cd nacos-config-example && mvn spring-boot:run

# 启动服务发现示例
cd nacos-discovery-example/nacos-naming-example && mvn spring-boot:run

# 启动 gRPC 连接内核示例
cd nacos-grpc-connection-example && mvn spring-boot:run

# 启动集群一致性示例
cd nacos-cluster-consistency-example && mvn spring-boot:run
```

## 项目结构

```
nacos-examples/
├── pom.xml                                              # 父 POM（全局依赖管理）
├── nacos-config-example/                                # 配置中心示例
│   ├── pom.xml
│   └── src/main/java/com/alibaba/nacos/example/
│       ├── NacosConfigExampleApplication.java           # 启动类
│       ├── config/
│       │   ├── DynamicConfigProperties.java             # @ConfigurationProperties
│       │   ├── LocalConfigSnapshotDemo.java             # 三级缓存演示
│       │   └── NacosConfigAnnotationDemo.java           # @NacosConfig 注解
│       ├── controller/
│       │   ├── AnnotationDemoController.java
│       │   ├── ConfigDemoController.java
│       │   ├── NacosApiAdvancedController.java          # 高级 API
│       │   └── NacosApiController.java
│       ├── listener/
│       │   ├── ConfigChangeMonitor.java
│       │   ├── GrpcPushReceiveDemo.java                 # gRPC 推送接收
│       │   └── NacosConfigListenerDemo.java
│       └── model/UserConfig.java
├── nacos-discovery-example/                             # 服务发现示例（聚合模块）
│   ├── pom.xml
│   ├── nacos-openfeign-*/                               # OpenFeign 集成
│   ├── nacos-dubbo-*/                                   # Dubbo 集成
│   └── nacos-naming-example/                            # NamingService API
│       ├── pom.xml
│       └── src/main/java/com/alibaba/nacos/example/naming/
│           ├── NacosNamingExampleApplication.java
│           ├── demo/
│           │   ├── NamingServiceDemo.java               # 注册/注销 API
│           │   └── ServiceSubscribeDemo.java            # 订阅 API
│           └── controller/NamingController.java
├── nacos-grpc-connection-example/                       # gRPC 连接内核
│   ├── pom.xml
│   └── src/main/java/com/alibaba/nacos/example/grpc/
│       ├── NacosGrpcConnectionExampleApplication.java
│       ├── demo/
│       │   ├── GrpcConnectionLifecycleDemo.java         # 连接生命周期
│       │   └── RpcRequestDispatchDemo.java              # RPC 请求调度
│       └── controller/GrpcConnectionController.java
└── nacos-cluster-consistency-example/                   # 集群一致性
    ├── pom.xml
    └── src/main/java/com/alibaba/nacos/example/cluster/
        ├── NacosClusterConsistencyExampleApplication.java
        ├── demo/
        │   ├── ClusterMemberManagerDemo.java            # 节点管理
        │   ├── DistroProtocolDemo.java                  # Distro 协议
        │   └── JRaftConsensusDemo.java                  # JRaft 共识
        └── controller/ClusterConsistencyController.java
```

## 注意事项

- 所有模块通过 Nacos Client API 与 Nacos Server 通信，需确保 Server 正常运行
- OpenFeign 和 Dubbo 模块需要同时启动 provider 和 consumer
- Mac M 系列芯片下 Dubbo 通信问题请参考 `README.md` 中"已知问题与解决方案"
