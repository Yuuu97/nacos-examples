package com.alibaba.nacos.example.naming.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
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
 * NamingService 核心 API 演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (七)：服务注册流程 —— 客户端上报与服务端存储》
 *
 * 演示 Nacos NamingService 的核心 API 使用方式，包括：
 * - 服务实例注册（临时/持久）
 * - 服务实例注销
 * - 服务实例查询（多种方式）
 * - 服务列表查询
 *
 * 内部调用链路（对应源码）：
 *   NamingService.registerInstance()
 *     → NacosNamingService.registerInstance()
 *       → NamingClientProxyDelegate.registerService()
 *         → NamingGrpcClientProxy.registerService()
 *           → registerServiceForEphemeral()  // 临时实例
 *             → redoService.cacheInstanceForRedo()
 *             → doRegisterService()
 *             → InstanceRequest(request=REGISTER_INSTANCE)
 *           → doRegisterServiceForPersistent()  // 持久实例
 *             → PersistentInstanceRequest(request=REGISTER_INSTANCE)
 *
 * @author qinyu
 */
@Component
public class NamingServiceDemo {

    private static final Logger log = LoggerFactory.getLogger(NamingServiceDemo.class);

    @Value("${spring.cloud.nacos.discovery.server-addr:127.0.0.1:8848}")
    private String serverAddr;

    @Value("${spring.cloud.nacos.discovery.namespace:public}")
    private String namespace;

    @Value("${spring.cloud.nacos.discovery.username:nacos}")
    private String username;

    @Value("${spring.cloud.nacos.discovery.password:nacos}")
    private String password;

    /**
     * 直接创建的 NamingService 实例（非 Spring Cloud 封装）
     */
    private NamingService namingService;

    /**
     * 注册操作计数器
     */
    private final AtomicInteger registerCount = new AtomicInteger(0);
    private final AtomicInteger deregisterCount = new AtomicInteger(0);

    /**
     * 已注册的实例记录
     */
    private final ConcurrentHashMap<String, List<Instance>> registeredInstances = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() throws NacosException {
        // 直接通过 NamingFactory 创建 NamingService 实例
        // 这对应 NacosNamingService 的构造过程
        Properties props = new Properties();
        props.setProperty("serverAddr", serverAddr);
        props.setProperty("namespace", namespace);
        props.setProperty("username", username);
        props.setProperty("password", password);
        this.namingService = NamingFactory.createNamingService(props);

        log.info("====== NamingServiceDemo 初始化 ======");
        log.info("NamingService 实例已创建 (serverAddr={})", serverAddr);
        log.info("NacosNamingService 构造链路对照：");
        log.info("  → NamingFactory.createNamingService(props)");
        log.info("    → new NacosNamingService(props)");
        log.info("      → NamingClientProxyDelegate(…)");
        log.info("        → NamingGrpcClientProxy(…)  // gRPC 客户端代理");
        log.info("          → RpcClient.start()  // 建立 gRPC 连接");
        log.info("=====================================");
    }

    /**
     * 注册一个服务实例（临时实例）
     *
     * 对应临时实例注册流程：
     *   NamingGrpcClientProxy.registerServiceForEphemeral()
     *     → redoService.cacheInstanceForRedo()  // Redo 缓存
     *     → doRegisterService()  // gRPC 发送 InstanceRequest
     *     → redoService.instanceRegistered()  // 标记已注册
     */
    public Instance registerInstance(String serviceName, String groupName, String ip, int port,
                                     String clusterName, double weight, Map<String, String> metadata) throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(weight);
        instance.setClusterName(clusterName);
        instance.setMetadata(metadata);
        instance.setEphemeral(true);  // 临时实例
        instance.setHealthy(true);

        // 调用 NamingService API 注册
        // 内部流程：
        //   registerInstance() → NacosNamingService.registerInstance()
        //     → NamingClientProxyDelegate.registerService()
        //       → NamingGrpcClientProxy.registerService()
        //         → registerServiceForEphemeral()  // ephemeral=true 走此分支
        //           → redoService.cacheInstanceForRedo()
        //           → doRegisterService()
        //             → InstanceRequest(type=REGISTER_INSTANCE)
        //             → requestToServer(InstanceRequest)
        //               → rpcClient.request()  // gRPC Unary 调用
        //           → redoService.instanceRegistered()
        namingService.registerInstance(serviceName, groupName, instance);

        int count = registerCount.incrementAndGet();
        String key = groupName + "@@" + serviceName;
        registeredInstances.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(instance);

        log.info("[实例注册 #{}] service={}@@{}, ip={}:{}, cluster={}, weight={}",
                count, groupName, serviceName, ip, port, clusterName, weight);
        log.info("注册流程追踪:");
        log.info("  → NamingService.registerInstance()");
        log.info("    → NacosNamingService.registerInstance()");
        log.info("    → NamingGrpcClientProxy.registerService()");
        log.info("      → registerServiceForEphemeral() (临时实例)");
        log.info("        → redoService.cacheInstanceForRedo() 缓存");
        log.info("        → doRegisterService() gRPC 发送 InstanceRequest");
        log.info("        → redoService.instanceRegistered() 标记成功");

        return instance;
    }

    /**
     * 注销服务实例
     *
     * 对应:
     *   InstanceRequestHandler.handle() → deregisterInstance()
     *     → EphemeralClientOperationServiceImpl.deregisterInstance()
     *       → client.removeServiceInstance()
     *       → NotifyCenter.publishEvent(ClientDeregisterServiceEvent)
     */
    public boolean deregisterInstance(String serviceName, String groupName, String ip, int port) throws NacosException {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setEphemeral(true);

        namingService.deregisterInstance(serviceName, groupName, instance);

        int count = deregisterCount.incrementAndGet();
        String key = groupName + "@@" + serviceName;
        List<Instance> instances = registeredInstances.get(key);
        if (instances != null) {
            instances.removeIf(i -> ip.equals(i.getIp()) && port == i.getPort());
        }

        log.info("[实例注销 #{}] service={}@@{}, ip={}:{}", count, groupName, serviceName, ip, port);
        return true;
    }

    /**
     * 查询服务实例列表
     *
     * 对应:
     *   NacosNamingService.selectInstances()
     *     → getServiceInfo() 三级降级策略
     *       ① Failover 容灾文件
     *       ② 订阅缓存或直连查询
     *     → ServiceUtil.selectInstancesWithHealthyProtection()
     */
    public List<Instance> getInstances(String serviceName, String groupName, boolean healthyOnly) throws NacosException {
        List<Instance> instances = namingService.selectInstances(serviceName, groupName, healthyOnly);
        log.info("查询服务实例: service={}@@{}, 返回 {} 个实例", groupName, serviceName, instances.size());
        return instances;
    }

    /**
     * 查询所有已订阅的服务列表
     */
    public ListView<String> getServicesOfServer(int pageNo, int pageSize) throws NacosException {
        return namingService.getServicesOfServer(pageNo, pageSize);
    }

    /**
     * 查询服务的所有实例（含不健康实例）
     */
    public List<Instance> getAllInstances(String serviceName, String groupName) throws NacosException {
        return namingService.getAllInstances(serviceName, groupName);
    }

    /**
     * 获取注册统计
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalRegister", registerCount.get());
        stats.put("totalDeregister", deregisterCount.get());
        stats.put("activeRegistrations", registeredInstances.size());
        Map<String, Integer> detail = new LinkedHashMap<>();
        registeredInstances.forEach((key, list) -> detail.put(key, list.size()));
        stats.put("registrations", detail);
        return stats;
    }

    @PreDestroy
    public void destroy() {
        if (namingService != null) {
            try {
                namingService.shutDown();
                log.info("NamingService 已关闭");
            } catch (NacosException e) {
                log.warn("关闭 NamingService 异常", e);
            }
        }
    }
}
