# Nacos gRPC 连接内核示例

> 配套文章：
>
> [《Nacos 2.x 源码深度解析 (九)：双向流设计 —— 连接创建复用与销毁》](https://blog.csdn.net/Yuu_9977/article/details/161889667?spm=1001.2014.3001.5501)
>
> [《Nacos 2.x 源码深度解析 (十)：心跳保活策略 —— 断线检测与重连源码》](https://blog.csdn.net/Yuu_9977/article/details/162269136?spm=1011.2415.3001.5331)
>
> [《Nacos 2.x 源码深度解析 (十一)：RPC 请求调度 —— 收发模型与线程池处理》](https://blog.csdn.net/Yuu_9977/article/details/162279139?spm=1011.2415.3001.5331)

本模块演示 Nacos gRPC 连接内核的核心机制，通过 Nacos Client API 间接展示连接生命周期、心跳保活和 RPC 请求调度。

## 项目结构

```
nacos-grpc-connection-example/
├── pom.xml
└── src/main/java/com/alibaba/nacos/example/grpc/
    ├── NacosGrpcConnectionExampleApplication.java    # 启动类
    ├── demo/
    │   ├── GrpcConnectionLifecycleDemo.java          # gRPC 连接生命周期
    │   └── RpcRequestDispatchDemo.java               # RPC 请求调度
    └── controller/
        └── GrpcConnectionController.java             # REST 接口
```

## 快速启动

```bash
cd nacos-grpc-connection-example
mvn spring-boot:run
```

## 测试接口

### 查看连接状态
```bash
curl "http://localhost:8086/grpc/connection/status"
```

### 连接详细信息
```bash
curl "http://localhost:8086/grpc/connection/info"
```

### 手动触发心跳检查
```bash
curl -X POST "http://localhost:8086/grpc/connection/check"
```

### RPC 调用统计
```bash
curl "http://localhost:8086/grpc/rpc/stats"
```

### 模拟同步 RPC 调用
```bash
curl -X POST "http://localhost:8086/grpc/rpc/sync"
```

### 查看事件日志
```bash
curl "http://localhost:8086/grpc/event-log"
```

## 核心链路

### 连接建立链路
```
RpcClient.start()
  → connectToServer(serverInfo)
    ① createNewManagedChannel(ip, port) 创建 TCP
    ② createNewChannelStub(channel) Unary Stub
    ③ serverCheck() ServerCheckRequest → connectionId
    ④ BiRequestStreamGrpc.newStub() 双向流 Stub
    ⑤ bindRequestStream() 绑定回调
    ⑥ 发送 ConnectionSetupRequest 握手
```

### 心跳保活链路
```
RpcClient 后台线程二 (每 5 秒):
  healthCheck() → HealthCheckRequest → 超时 3 秒
    → 成功 → 更新 lastActiveTimeStamp
    → 连续失败 → UNHEALTHY → reconnect()
      → 退避重试 (100ms ~ 5s)
```

### RPC 请求调度链路
```
客户端:
  RpcClient.request() [同步, 重试 3 次, UN_REGISTER 切换]
  RpcClient.asyncRequest() [Futures.addCallback + withTimeout]

服务端:
  GrpcRequestAcceptor.request()
    → requestHandlerRegistry.getByRequestType() [Handler 路由]
    → connectionManager.checkValid() [连接验证]
    → RequestHandler.handleRequest()
      → TpsControlRequestFilter [限流]
      → RemoteRequestAuthFilter [鉴权]
      → RemoteParamCheckFilter [校验]
      → handle() [业务逻辑]
```

## 源码对照

| 文章章节 | 核心类/方法 | 示例代码对应 |
|---------|------------|------------|
| §9 连接建立 | `GrpcClient.connectToServer()` | `GrpcConnectionLifecycleDemo.init()` |
| §9 三层复用 | RpcClientFactory / GrpcClient / GrpcConnection | `getConnectionInfo()` 架构说明 |
| §9 服务端启动 | `BaseGrpcServer.startServer()` | 连接信息注释 |
| §10 心跳保活 | `RpcClient.healthCheck()` | `simulateHeartBeat()` |
| §10 重连 | `RpcClient.reconnect()` | 重连链路注释 |
| §10 Redo 补偿 | `NamingGrpcRedoService` | Redo 注释 |
| §11 三种请求模式 | `RpcClient.request/asyncRequest/requestFuture` | `RpcRequestDispatchDemo` |
| §11 双线程池 | `GlobalExecutor.sdkRpcExecutor / clusterRpcExecutor` | RPC 统计中的说明 |
| §11 ACK 同步 | `RpcAckCallbackSynchronizer` | 推送注释 |