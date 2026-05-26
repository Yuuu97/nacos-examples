package com.alibaba.nacos.example.service;

import com.alibaba.nacos.example.api.User;
import com.alibaba.nacos.example.api.UserService;
import org.apache.dubbo.config.annotation.DubboService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * UserService 的 Dubbo 实现，通过 {@code @DubboService} 暴露为 Dubbo RPC 端点并注册到 Nacos。
  * @author qinyu
 */
@DubboService(version = "1.0.0", group = "DEFAULT_GROUP", interfaceClass = UserService.class)
public class UserServiceImpl implements UserService {
    private static final Logger log = LoggerFactory.getLogger(UserServiceImpl.class);

    @Value("${server.port:8083}")
    private int httpPort;

    @Override
    public User getUser(Long id) {
        log.info("[Dubbo Provider] 收到 RPC 请求: getUser({})", id);
        User user = new User(id, "user-" + id, "user" + id + "@example.com", 20 + (int) (id % 20));
        log.info("[Dubbo Provider] 返回用户: {}", user);
        return user;
    }

    @Override
    public Map<String, Object> getInstanceInfo() {
        Map<String, Object> info = new LinkedHashMap<>();
        try {
            String ip = InetAddress.getLocalHost().getHostAddress();
            info.put("type", "Dubbo Provider");
            info.put("ip", ip);
            info.put("httpPort", httpPort);
            info.put("dubboPort", 20880);
            info.put("protocol", "dubbo");
            info.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } catch (Exception e) {
            log.error("获取实例信息失败", e);
            info.put("error", e.getMessage());
        }
        return info;
    }

    @Override
    public String health() {
        return "OK - Dubbo Provider is running";
    }
}
