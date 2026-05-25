package com.alibaba.nacos.example.model;

import java.io.Serializable;

/**
 * Nacos 配置模型类 — 用于演示 @NacosConfig / @NacosConfigListener 的 JSON 反序列化能力
 *
 * <p><b>设计思想：配置驱动（Configuration-Driven）</b></p>
 * 将 Nacos 中存储的 JSON 配置直接映射为 Java 对象，实现"配置即对象"。
 * Nacos 控制台修改 JSON → 注解自动反序列化 → 引用自动更新，无需手动解析。
 *
 * <p><b>对应 Nacos 配置（需要在 Nacos 控制台创建）：</b></p>
 * <pre>
 *   dataId: user-config.json
 *   group:   DEFAULT_GROUP
 *   内容示例:
 *   {
 *     "username": "admin",
 *     "password": "123456",
 *     "age": 25,
 *     "email": "admin@example.com",
 *     "roles": ["ROLE_ADMIN", "ROLE_USER"],
 *     "metadata": {
 *       "department": "engineering",
 *       "level": "senior"
 *     }
 *   }
 * </pre>
 *
 * <p><b>调用链路：</b></p>
 * <pre>
 *   应用启动 → NacosConfigAnnotationProcessor 扫描 @NacosConfig 字段
 *     → NacosConfigManager.getConfigService().getConfig(dataId, group)
 *     → 通过 {@link com.alibaba.cloud.nacos.annotation.AbstractConfigChangeListener}
 *        注册 gRPC 长轮询监听
 *     → Nacos Server 推送变更 → 字段值自动刷新
 * </pre>
 *
 * <p><b>观察者模式体现：</b></p>
 * <ul>
 *   <li><b>Subject（主题）</b>：Nacos Server 上的 user-config.json 配置</li>
 *   <li><b>Observer（观察者）</b>：标注了 @NacosConfig 的字段 + @NacosConfigListener 方法</li>
 *   <li><b>通知机制</b>：gRPC BiRequestStream 双向流 → handleConfigChange → 批量 MD5 校验 → 字段注入</li>
 * </ul>
 *
 * @author nacos-examples
 */
public class UserConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String username;
    private String password;
    private int age;
    private String email;
    private String[] roles;
    private java.util.Map<String, String> metadata;

    public UserConfig() {
    }

    // ==================== Getter / Setter ====================

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String[] getRoles() {
        return roles;
    }

    public void setRoles(String[] roles) {
        this.roles = roles;
    }

    public java.util.Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(java.util.Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "UserConfig{" +
                "username='" + username + '\'' +
                ", password='" + (password != null ? "******" : "null") + '\'' +
                ", age=" + age +
                ", email='" + email + '\'' +
                ", roles=" + java.util.Arrays.toString(roles) +
                ", metadata=" + metadata +
                '}';
    }
}
