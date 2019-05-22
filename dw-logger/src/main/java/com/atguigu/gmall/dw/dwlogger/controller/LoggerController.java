package com.atguigu.gmall.dw.dwlogger.controller;



import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.dw.constant.GmallConstant;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("/log")
    public String doLog(@RequestParam("log") String log){
        //1.补时间戳
        JSONObject logJSON = JSON.parseObject(log);
        logJSON.put("ts", System.currentTimeMillis());

        //2.落盘log
        String jsonString = logJSON.toJSONString();
        logger.info(jsonString);



        //3. 启动日志和事件日志分流，分到不同的Kafka Topic中去
        if ("startup".equals(logJSON.getString("type"))){
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_STARTUP, jsonString);
        }else {
            kafkaTemplate.send(GmallConstant.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }
}
