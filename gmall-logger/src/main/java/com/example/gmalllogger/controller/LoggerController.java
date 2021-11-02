package com.example.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Desription:
 *
 * @ClassName LoggerController
 * @Author Zhanyuwei
 * @Date 2021/10/30 16:15
 * @Version 1.0
 **/
@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    @GetMapping("/test")
    public String test1(@RequestParam("name") String name,
                        @RequestParam(value = "age",defaultValue = "18") int age){
        System.out.println("success");
        return "Success";
    }

    @GetMapping("/applog")
    public String getLog(@RequestParam("param") String jsonStr){

        // 打印数据
        //System.out.println(jsonStr);

        // 数据落盘
        log.info(jsonStr);

        // 将数据写入kafka
        kafkaTemplate.send("ods_base_log",jsonStr);
        return "Success";
    }
}
