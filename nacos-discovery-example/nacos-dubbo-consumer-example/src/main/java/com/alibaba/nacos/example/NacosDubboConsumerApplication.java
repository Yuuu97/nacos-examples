package com.alibaba.nacos.example;

import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.lang.reflect.Field;
import java.net.InetAddress;

/**
 * Dubbo 服务消费者启动类。
 *
 * <p>在 macOS 多网卡环境下（如 VirtualBox vboxnet0），通过反射强制设置
 * {@code NetUtils} 静态缓存，避免 Dubbo 日志中的 current host 误报为虚拟网卡 IP。
  * @author qinyu
 */
@EnableDubbo
@SpringBootApplication
public class NacosDubboConsumerApplication {

    private static final String KEY_IP_TO_BIND = "dubbo.ip.to.bind";
    private static final String KEY_PROTOCOL_HOST = "dubbo.protocol.host";
    private static final String KEY_APPLICATION_HOST = "dubbo.application.host";
    private static final String KEY_IGNORED_INTERFACES = "dubbo.network.ignored.interfaces";
    private static final String LOCALHOST = "127.0.0.1";

    public static void main(String[] args) {
        forceNetUtilsHost();

        setPropertyIfAbsent(KEY_IP_TO_BIND, LOCALHOST);
        setPropertyIfAbsent(KEY_PROTOCOL_HOST, LOCALHOST);
        setPropertyIfAbsent(KEY_APPLICATION_HOST, LOCALHOST);
        setPropertyIfAbsent(KEY_IGNORED_INTERFACES, "vboxnet,vboxnet0,docker,vmnet,utun,bridge");

        SpringApplication.run(NacosDubboConsumerApplication.class, args);
        System.out.println("\n==========================================");
        System.out.println("  Dubbo Consumer 启动成功");
        System.out.println("  HTTP  端口: 8084 (REST 接口)");
        System.out.println("  Nacos 注册中心: 127.0.0.1:8848");
        System.out.println("  订阅服务: dubbo-provider-example");
        System.out.println("==========================================\n");
    }

    /**
     * 反射设置 {@code NetUtils.HOST_ADDRESS} 和 {@code LOCAL_ADDRESS} 为 127.0.0.1。
     */
    private static void forceNetUtilsHost() {
        try {
            Field hostField = NetUtils.class.getDeclaredField("HOST_ADDRESS");
            hostField.setAccessible(true);
            if (hostField.get(null) == null) {
                hostField.set(null, LOCALHOST);
            }

            Field addrField = NetUtils.class.getDeclaredField("LOCAL_ADDRESS");
            addrField.setAccessible(true);
            if (addrField.get(null) == null) {
                addrField.set(null, InetAddress.getByName(LOCALHOST));
            }
        } catch (Exception e) {
            System.err.println("[Dubbo Host Fix] 反射设置 NetUtils 缓存失败: " + e.getMessage());
        }
    }

    /**
     * 安全设置系统属性，仅在尚未设置时写入。
     */
    private static void setPropertyIfAbsent(String key, String value) {
        String existing = System.getProperty(key);
        if (existing == null || existing.isEmpty()) {
            System.setProperty(key, value);
        }
    }
}
