package com.alibaba.nacos.example.api;

import java.util.Map;

/**
 * Dubbo 服务接口。Provider 实现此接口并注册到 Nacos，Consumer 通过 Dubbo 协议远程调用。
  * @author qinyu
 */
public interface UserService {

    /**
     * 根据 ID 查询用户。
     */
    User getUser(Long id);

    /**
     * 返回当前 Provider 实例信息，用于验证 Nacos 注册和负载均衡效果。
     */
    Map<String, Object> getInstanceInfo();

    /**
     * 健康检查。
     */
    String health();
}
