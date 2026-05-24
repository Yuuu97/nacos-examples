package com.alibaba.nacos.example.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Configuration;

/**
 * 动态配置属性类 — 演示 @ConfigurationProperties + @RefreshScope 绑定
 *
 * <p>对应文章核心机制：</p>
 * <ul>
 *   <li><b>启动配置拉取：</b>NacosConfigDataLoader.load() → ConfigService.getConfig()
 *       拉取 Nacos Server 上的 dynamic-config.yml，注入 Environment，
 *       其属性值绑定到本类的 name / version / refreshInterval 字段</li>
 *   <li><b>动态刷新：</b>Nacos 控制台修改配置 → gRPC 推送变更通知
 *       → NacosContextRefresher 发布 NacosConfigRefreshEvent
 *       → NacosConfigRefreshEventListener 转换为 RefreshEvent
 *       → @RefreshScope 标注的 Bean 被销毁并基于新配置重建</li>
 * </ul>
 *
 * <p>对应源码链路：</p>
 * <pre>
 *   NacosConfigDataLoader.doLoad()
 *     → NacosPropertySourceRepository.collectNacosPropertySource()
 *     → ConfigData(propertySources) → Environment
 *     → ConfigurationPropertiesBindingPostProcessor.bind()
 *     → 注入到本类字段
 * </pre>
 *
 * @author nacos-examples
 */
@Configuration
@ConfigurationProperties(prefix = "app")
@RefreshScope
public class DynamicConfigProperties {

    /**
     * 应用名称（对应配置项 app.name）
     * 修改 Nacos 上 dynamic-config.yml 中的 app.name 后自动刷新
     */
    private String name;

    /**
     * 应用版本（对应配置项 app.version）
     * 修改 Nacos 上 dynamic-config.yml 中的 app.version 后自动刷新
     */
    private String version;

    /**
     * 定时刷新间隔（毫秒），对应配置项 app.refresh-interval
     */
    private long refreshInterval;

    // ========== Getters & Setters ==========

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(long refreshInterval) {
        this.refreshInterval = refreshInterval;
    }
}
