# Nacos NamingService API 示例

> 配套文章：
>
> [《Nacos 2.x 源码深度解析 (七)：服务注册流程 —— 客户端上报与服务端存储》](https://blog.csdn.net/Yuu_9977/article/details/161495802?spm=1001.2014.3001.5502)
>
> [《Nacos 2.x 源码深度解析 (八)：服务订阅机制 —— 从首次订阅到gRPC双向流变更通知》](https://blog.csdn.net/Yuu_9977/article/details/161665254?spm=1011.2415.3001.5331)

本模块演示 Nacos NamingService 核心 API 的调用方式，通过 REST 接口展示服务注册、注销、订阅等操作。

## 项目结构

```
nacos-naming-example/
├── pom.xml
└── src/main/java/com/alibaba/nacos/example/naming/
    ├── NacosNamingExampleApplication.java    # 启动类
    ├── demo/
    │   ├── NamingServiceDemo.java            # 注册/注销/查询 API
    │   └── ServiceSubscribeDemo.java         # 订阅/取消订阅 API
    └── controller/
        └── NamingController.java             # REST 接口
```

## 快速启动

```bash
cd nacos-discovery-example/nacos-naming-example
mvn spring-boot:run
```

## 测试接口

### 注册实例
```bash
curl -X POST "http://localhost:8085/naming/register?serviceName=my-service&port=8081"
```

### 查询实例
```bash
curl "http://localhost:8085/naming/instances?serviceName=my-service"
```

### 订阅服务
```bash
curl -X POST "http://localhost:8085/naming/subscribe?serviceName=my-service"
```

### 取消订阅
```bash
curl -X POST "http://localhost:8085/naming/unsubscribe?serviceName=my-service"
```

## 源码对照

| 文章章节 | 核心类/方法 | 示例代码对应 |
|---------|------------|------------|
| §7 服务注册 | `NamingGrpcClientProxy.registerService()` | `NamingServiceDemo.registerInstance()` |
| §7 Redo 机制 | `NamingGrpcRedoService` | 注册流程注释 |
| §7 服务端接收 | `InstanceRequestHandler.handle()` | 注册链路注释 |
| §7 注册表三层 | `ServiceManager` / `AbstractClient` / `ClientServiceIndexesManager` | 接口文档注释 |
| §8 订阅流程 | `NamingClientProxyDelegate.subscribe()` | `ServiceSubscribeDemo.subscribe()` |
| §8 推送接收 | `NamingPushRequestHandler.requestReply()` | 变更通知回调 |
| §8 三级降级 | Failover → 订阅缓存 → 直连 | `getServiceInfo()` 流程注释 |