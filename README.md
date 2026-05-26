nacos-examples是《Nacos 2.x源码深度解析》专栏中用于学习Nacos的配套示例代码。请读者根据专栏中的篇章找到对应的module进行学习。 由于作者是在工作之余写的文章及示例代码，如有勘误，请大家多多指正并提出宝贵的意见，谢谢。

> ### nacos-config-example：
>
> ***一、架构通信篇：***
> [《Nacos 2.x 源码深度解析 (一)：架构整体全貌 —— 核心模块划分与版本演进》](https://blog.csdn.net/Yuu_9977/article/details/161167369?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (一)：架构整体全貌 —— 核心模块划分与版本演进")
> [《Nacos 2.x 源码深度解析 (二)：通信协议迭代 —— HTTP长轮询到gRPC演进》](https://blog.csdn.net/Yuu_9977/article/details/161397929?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (二)：通信协议迭代 —— HTTP长轮询到gRPC演进")
>
> ***二、配置中心篇***
> [《Nacos 2.x 源码深度解析 (三)：配置中心客户端 —— 启动加载与自动装配》](https://blog.csdn.net/Yuu_9977/article/details/161373008?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (三)：配置中心客户端 —— 启动加载与自动装配")
> [《Nacos 2.x 源码深度解析 (四)：配置中心服务端 —— 事件总线与数据持久化》](https://blog.csdn.net/Yuu_9977/article/details/161400534?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (四)：配置中心服务端 —— 事件总线与数据持久化")
> [《Nacos 2.x 源码深度解析 (五)：gRPC 推送链路 —— 配置变更下发与动态刷新》](https://blog.csdn.net/Yuu_9977/article/details/161401011?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (五)：gRPC 推送链路 —— 配置变更下发与动态刷新")
> [《Nacos 2.x 源码深度解析 (六)：三级缓存体系 —— 降级兜底与故障自愈机制》](https://blog.csdn.net/Yuu_9977/article/details/161401280?spm=1001.2014.3001.5502 "Nacos 2.x 源码深度解析 (六)：三级缓存体系 —— 降级兜底与故障自愈机制")

---

## 已知问题与解决方案

### Mac M 系列芯片下 Dubbo Consumer 无法与 Provider 通信

**问题现象**：在搭载 Apple Silicon M芯片 的 Mac 上运行 `nacos-dubbo-consumer-example` 和 `nacos-dubbo-provider-example` 时，Consumer 无法通过 Dubbo 协议与 Provider 建立 RPC 通信，即使双方均已成功注册到 Nacos 注册中心。

**根因分析**：

1. **绑定地址问题**：Mac M 系列芯片的虚拟化网络环境下，系统默认 hostname 可能解析为类似 `10.251.1.1` 的非回环地址。Dubbo 3.x 默认使用 Triple 协议（基于 gRPC/Protobuf），Provider 启动时会自动绑定到该地址。Consumer 从 Nacos 拿到 Provider 注册的地址后尝试连接 `10.251.1.1:20880`，但由于该地址实际不可路由或不在同一网段，导致连接超时。

2. **Protobuf 版本兼容性**：Dubbo 3.x Triple 协议依赖 Protobuf 进行序列化。低版本的 `protobuf-java`（如 Dubbo 默认传递的 3.19.x）在 macOS ARM64 (aarch64) 架构下存在兼容性问题，可能导致 gRPC 双向流建立失败。需要显式升级到 `3.22.0` 或更高版本。

**解决方案**：

#### 一：启动时反射劫持 `NetUtils` + 系统属性强制绑定（推荐）

> 仅通过 `application.yml` 中的 `dubbo.protocol.host` 或启动 `-D` 参数指定绑定地址**不生效**。
> 原因：Dubbo 内部通过 `org.apache.dubbo.common.utils.NetUtils.getLocalHost()` 获取本机地址，
> 该方法依赖静态缓存字段 `HOST_ADDRESS` 和 `LOCAL_ADDRESS`，这些缓存可能在 Dubbo 配置加载之前就已初始化。
> 必须在 Spring 容器启动前，通过反射预先注入本地回环地址。

在两个 Dubbo 模块的启动类 `main()` 中，于 `SpringApplication.run()` 之前加入以下代码：

**Provider 参考**：`nacos-dubbo-provider-example/.../NacosDubboProviderApplication.java`
**Consumer 参考**：`nacos-dubbo-consumer-example/.../NacosDubboConsumerApplication.java`

```java
import org.apache.dubbo.common.utils.NetUtils;
import java.lang.reflect.Field;
import java.net.InetAddress;

public static void main(String[] args) {
    // 1) 反射强制设置 NetUtils 静态缓存，确保 Dubbo 获取到的本地地址为 127.0.0.1
    forceNetUtilsHost();

    // 2) 设置 Dubbo 绑定相关的系统属性（仅在未通过 -D 参数设置时写入）
    setPropertyIfAbsent("dubbo.ip.to.bind", "127.0.0.1");
    setPropertyIfAbsent("dubbo.protocol.host", "127.0.0.1");
    setPropertyIfAbsent("dubbo.application.host", "127.0.0.1");

    // 3) 忽略 macOS 多网卡环境下的虚拟网卡接口，防止 getLocalHost() 误取
    setPropertyIfAbsent("dubbo.network.ignored.interfaces",
            "vboxnet,vboxnet0,docker,vmnet,utun,bridge");

    SpringApplication.run(Application.class, args);
}

/**
 * 在 Dubbo NetUtils 首次初始化前，通过反射将 HOST_ADDRESS
 * 和 LOCAL_ADDRESS 静态字段预置为 127.0.0.1。
 */
private static void forceNetUtilsHost() {
    try {
        Field hostField = NetUtils.class.getDeclaredField("HOST_ADDRESS");
        hostField.setAccessible(true);
        if (hostField.get(null) == null) {
            hostField.set(null, "127.0.0.1");
        }

        Field addrField = NetUtils.class.getDeclaredField("LOCAL_ADDRESS");
        addrField.setAccessible(true);
        if (addrField.get(null) == null) {
            addrField.set(null, InetAddress.getByName("127.0.0.1"));
        }
    } catch (Exception e) {
        System.err.println("[Dubbo Host Fix] 反射设置 NetUtils 缓存失败: " + e.getMessage());
    }
}

/**
 * 安全设置系统属性：仅在尚未设置时写入，命令行 -D 参数传入的值优先级最高。
 */
private static void setPropertyIfAbsent(String key, String value) {
    if (System.getProperty(key) == null || System.getProperty(key).isEmpty()) {
        System.setProperty(key, value);
    }
}
```

> `dubbo.network.ignored.interfaces` 用于排除 VirtualBox、Docker、Parallels、VPN（utun）等虚拟网卡，
> 配合反射设置 `NetUtils.HOST_ADDRESS`，确保 Dubbo 日志中的 `current host` 和注册到 Nacos 的地址均为 `127.0.0.1`。

#### 二：升级 Protobuf 版本

在根 `pom.xml` 中统一管理 Protobuf 版本，并显式声明依赖：

```xml
<properties>
    <protobuf-java.version>3.22.0</protobuf-java.version>
</properties>

<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.google.protobuf</groupId>
            <artifactId>protobuf-java</artifactId>
            <version>${protobuf-java.version}</version>
        </dependency>
    </dependencies>
</dependencyManagement>
```

同时确保各 Dubbo 子模块的 `pom.xml` 中也声明了 `protobuf-java` 依赖：
```xml
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>${protobuf-java.version}</version>
</dependency>
```

