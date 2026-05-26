package com.alibaba.nacos.example.config;

import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.ProtocolConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class DubboConfig {

    @Bean
    public ApplicationConfig applicationConfig() {
        ApplicationConfig config = new ApplicationConfig();
        config.setName("dubbo-provider-example");
        config.setLogger("slf4j");
        config.setQosEnable(false);
        return config;
    }

    @Bean
    public ProtocolConfig protocolConfig() {
        ProtocolConfig config = new ProtocolConfig();
        config.setName("tri");
        config.setPort(20880);
        config.setHost("127.0.0.1");
        config.setSerialization("fastjson2");
        return config;
    }

    @Bean
    public RegistryConfig registryConfig() {
        RegistryConfig config = new RegistryConfig();
        config.setAddress("nacos://127.0.0.1:8848");
        config.setProtocol("nacos");
        config.setPort(8848);
        config.setTimeout(5000);
        config.setRegister(true);
        config.setSubscribe(true);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("namespace", "public");
        parameters.put("group", "DEFAULT_GROUP");
        parameters.put("username", "nacos");
        parameters.put("password", "nacos");
        config.setParameters(parameters);
        return config;
    }

    @Bean
    public MetadataReportConfig metadataReportConfig() {
        MetadataReportConfig config = new MetadataReportConfig();
        config.setAddress("nacos://127.0.0.1:8848");
        config.setTimeout(5000);
        config.setRetryTimes(3);
        config.setCycleReport(true);
        config.setSyncReport(true);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("namespace", "public");
        parameters.put("group", "DEFAULT_GROUP");
        parameters.put("username", "nacos");
        parameters.put("password", "nacos");
        config.setParameters(parameters);

        return config;
    }

    @Bean
    public ProviderConfig providerConfig() {
        ProviderConfig config = new ProviderConfig();
        config.setVersion("1.0.0");
        config.setGroup("DEFAULT_GROUP");
        config.setTimeout(5000);
        config.setRetries(0);
        config.setDelay(-1);
        config.setLoadbalance("roundrobin");
        return config;
    }
}
