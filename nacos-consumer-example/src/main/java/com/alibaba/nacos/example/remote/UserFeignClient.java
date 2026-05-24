package com.alibaba.nacos.example.remote;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "nacos-provider-example", path = "/user")
public interface UserFeignClient {
    
    @GetMapping("/hello")
    String hello();
    
    @GetMapping("/info")
    String getUserInfo();
    
    @GetMapping("/detail/{id}")
    String getUserById(@PathVariable("id") Long id);
    
    @GetMapping("/query")
    String queryUser(@RequestParam("name") String name);
}