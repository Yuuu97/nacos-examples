package com.alibaba.nacos.example.controller;

import com.alibaba.nacos.example.remote.UserFeignClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class ConsumerController {
    
    @Autowired
    private UserFeignClient userFeignClient;
    
    @GetMapping("/hello")
    public String hello() {
        return userFeignClient.hello();
    }
}