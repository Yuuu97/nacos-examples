package com.alibaba.nacos.example.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/user")
public class UserController {
    
    @GetMapping("/hello")
    public String hello() {
        return "Hello, 我是用户服务！";
    }
    
    @GetMapping("/info")
    public String getUserInfo() {
        return "用户信息：张三，VIP会员";
    }
    
    @GetMapping("/detail/{id}")
    public String getUserById(@PathVariable("id") Long id) {
        return "用户ID: " + id + "，姓名：张三，邮箱：zhangsan@example.com";
    }
    
    @GetMapping("/query")
    public String queryUser(@RequestParam("name") String name) {
        return "查询用户：" + name + "，状态：正常";
    }
}