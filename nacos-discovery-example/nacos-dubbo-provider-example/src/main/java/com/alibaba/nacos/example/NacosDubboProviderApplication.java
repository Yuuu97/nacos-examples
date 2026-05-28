package com.alibaba.nacos.example;

import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.config.spring.context.annotation.EnableDubbo;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

import java.lang.reflect.Field;
import java.net.InetAddress;

/**
 * Dubbo 服务提供者启动类。
 *
 * <p>以 Spring Cloud 为服务治理核心，Dubbo 3.3.0 作为 RPC 通信框架。
 * 通过 {@code @EnableDiscoveryClient} 启用 Spring Cloud 服务发现，
 * 与 {@code @EnableDubbo} 配合实现 Dubbo 与 Spring Cloud 的双重注册。
 *
 * <p>在 macOS 多网卡环境下（如 VirtualBox vboxnet0），通过反射强制设置
 * {@code NetUtils} 静态缓存，避免 Dubbo 日志中的 current host 误报为虚拟网卡 IP。
 * 同时忽略 VirtualBox/Docker/Parallels 等虚拟网卡接口。
 *
 * @author qinyu
 */
@EnableDubbo
@EnableDiscoveryClient
@SpringBootApplication
public class NacosDubboProviderApplication {

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

        SpringApplication.run(NacosDubboProviderApplication.class, args);
        System.out.println("\n==========================================");
        System.out.println("  Dubbo Provider 启动成功");
        System.out.println("  HTTP  端口: 8083 (管理端点)");
        System.out.println("  Dubbo 端口: 20880 (RPC 协议)");
        System.out.println("  Nacos 注册中心: 127.0.0.1:8848");
        System.out.println("==========================================\n");
    }

    /**
     * 反射设置 {@code NetUtils.HOST_ADDRESS} 和 {@code LOCAL_ADDRESS} 为 127.0.0.1，
     * 使 {@code getLocalHost()} 在 Dubbo 组件初始化前就返回正确的本地地址。
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
     * 安全设置系统属性，仅在尚未设置时写入，已通过 -D 参数设置的值不会被覆盖。
     */
    private static void setPropertyIfAbsent(String key, String value) {
        String existing = System.getProperty(key);
        if (existing == null || existing.isEmpty()) {
            System.setProperty(key, value);
        }
    }
}
