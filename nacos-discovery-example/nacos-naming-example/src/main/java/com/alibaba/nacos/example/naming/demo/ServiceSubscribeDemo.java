package com.alibaba.nacos.example.naming.demo;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.Event;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务订阅演示
 *
 * 对应文章《Nacos 2.x 源码深度解析 (八)：服务订阅机制 —— 从首次订阅到gRPC双向流变更通知》
 *
 * 演示 Nacos 服务订阅机制的完整链路：
 *
 * 订阅触发链路：
 *   NamingService.subscribe()
 *     → NamingClientProxyDelegate.subscribe()
 *       → serviceInfoUpdateService.scheduleUpdateIfAbsent()  // 定时兜底
 *       → grpcClientProxy.subscribe()  // gRPC Unary 订阅
 *         → SubscribeServiceRequest(subscribe=true)
 *         → requestToServer()
 *   // 服务端:
 *   SubscribeServiceRequestHandler.handle()
 *     → EphemeralClientOperationServiceImpl.subscribeService()
 *       → client.addServiceSubscriber()
 *       → NotifyCenter.publishEvent(ClientSubscribeServiceEvent)
 *         → ClientServiceIndexesManager → ServiceSubscribedEvent
 *           → NamingSubscriberServiceV2Impl → PushDelayTask(500ms)
 *
 * 变更通知链路：
 *   服务端 PushExecuteTask → RpcPushService.pushWithCallback()
 *     → gRPC BiRequestStream → 客户端
 *   NamingPushRequestHandler.requestReply()
 *     → serviceInfoHolder.processServiceInfo()
 *       → InstancesChangeEvent
 *         → InstancesChangeNotifier.onEvent()
 *           → EventListener.onEvent()  ← 此处触发回调
 *
 * @author qinyu
 */
@Component
public class ServiceSubscribeDemo {

    private static final Logger log = LoggerFactory.getLogger(ServiceSubscribeDemo.class);

    @Autowired
    private NamingServiceDemo namingServiceDemo;

    /**
     * 当前活跃的订阅
     */
    private final ConcurrentHashMap<String, SubscriberEntry> subscribers = new ConcurrentHashMap<>();

    private final AtomicInteger subscribeCount = new AtomicInteger(0);

    @PostConstruct
    public void init() {
        log.info("====== ServiceSubscribeDemo 初始化 ======");
        log.info("组件已就绪，支持服务订阅与变更通知");
        log.info("========================================");
    }

    /**
     * 订阅服务
     *
     * @return 订阅 ID
     */
    public String subscribe(String serviceName, String groupName) throws NacosException {
        String key = groupName + "@@" + serviceName;
        if (subscribers.containsKey(key)) {
            return "already_subscribed:" + key;
        }

        NamingService namingService = getNamingService();
        SubscriberEntry entry = new SubscriberEntry(serviceName, groupName);
        entry.listener = event -> {
            if (event instanceof NamingEvent) {
                NamingEvent namingEvent = (NamingEvent) event;
                List<Instance> instances = namingEvent.getInstances();
                entry.notifyCount.incrementAndGet();
                entry.lastNotifyTime = System.currentTimeMillis();
                entry.lastInstances = instances;

                log.info("================================================");
                log.info("[服务变更通知 #{}] service={}@@{}",
                        entry.notifyCount.get(), groupName, serviceName);
                log.info("  当前实例数: {}", instances != null ? instances.size() : 0);
                if (instances != null) {
                    for (Instance inst : instances) {
                        log.info("    -> {}:{}  healthy={}, weight={}, cluster={}",
                                inst.getIp(), inst.getPort(),
                                inst.isHealthy(), inst.getWeight(), inst.getClusterName());
                    }
                }
                log.info("================================================");
                log.info("推送接收链路追踪:");
                log.info("  ① Server PushExecuteTask → RpcPushService.pushWithCallback()");
                log.info("     → gRPC BiRequestStream.onNext(Payload)");
                log.info("  ② Client NamingPushRequestHandler.requestReply()");
                log.info("     → serviceInfoHolder.processServiceInfo()");
                log.info("       → InstancesDiff → InstancesChangeEvent");
                log.info("  ③ InstancesChangeNotifier.onEvent()");
                log.info("     → EventListener.onEvent() ← 当前回调");
            }
        };

        namingService.subscribe(serviceName, groupName, entry.listener);
        subscribers.put(key, entry);
        int count = subscribeCount.incrementAndGet();

        log.info("[服务订阅 #{}] service={}@@{}", count, groupName, serviceName);
        log.info("订阅流程追踪:");
        log.info("  → NamingService.subscribe()");
        log.info("    → NamingClientProxyDelegate.subscribe()");
        log.info("      → serviceInfoUpdateService.scheduleUpdateIfAbsent() 定时兜底");
        log.info("      → grpcClientProxy.subscribe() gRPC Unary 订阅");
        log.info("        → SubscribeServiceRequest(subscribe=true)");
        log.info("        → requestToServer()");

        return "subscribed:" + key;
    }

    /**
     * 取消订阅
     */
    public boolean unsubscribe(String serviceName, String groupName) throws NacosException {
        String key = groupName + "@@" + serviceName;
        SubscriberEntry entry = subscribers.remove(key);
        if (entry == null) {
            return false;
        }

        NamingService namingService = getNamingService();
        namingService.unsubscribe(serviceName, groupName, entry.listener);

        log.info("[取消订阅] service={}@@{}, 共接收 {} 次变更通知", groupName, serviceName, entry.notifyCount.get());
        return true;
    }

    /**
     * 查询已订阅的服务
     */
    public List<Map<String, Object>> getSubscribedServices() {
        List<Map<String, Object>> list = new ArrayList<>();
        subscribers.forEach((key, entry) -> {
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("serviceName", entry.serviceName);
            info.put("groupName", entry.groupName);
            info.put("notifyCount", entry.notifyCount.get());
            info.put("lastNotifyTime", entry.lastNotifyTime > 0 ?
                    new Date(entry.lastNotifyTime).toString() : "无");
            info.put("currentInstances", entry.lastInstances != null ? entry.lastInstances.size() : 0);
            list.add(info);
        });
        return list;
    }

    private NamingService getNamingService() {
        // 通过反射或直接注入的方式获取 NamingService
        // 简化处理：这里直接从 NamingServiceDemo 获取
        try {
            var field = NamingServiceDemo.class.getDeclaredField("namingService");
            field.setAccessible(true);
            return (NamingService) field.get(namingServiceDemo);
        } catch (Exception e) {
            throw new RuntimeException("无法获取 NamingService 实例", e);
        }
    }

    static class SubscriberEntry {
        final String serviceName;
        final String groupName;
        EventListener listener;
        final AtomicInteger notifyCount = new AtomicInteger(0);
        long lastNotifyTime;
        List<Instance> lastInstances;

        SubscriberEntry(String serviceName, String groupName) {
            this.serviceName = serviceName;
            this.groupName = groupName;
        }
    }
}
