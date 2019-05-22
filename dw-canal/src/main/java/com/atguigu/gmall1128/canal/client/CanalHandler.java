package com.atguigu.gmall1128.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.dw.constant.GmallConstant;
import com.atguigu.gmall1128.canal.util.MyKafkaSender;
import com.google.common.base.CaseFormat;

import java.util.List;

public class CanalHandler {
    public static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        // 判断业务类型
        if ("order_info".equals(tableName)&&eventType.equals(CanalEntry.EventType.INSERT)&&rowDatasList!=null&&rowDatasList.size()!=0){
            for (CanalEntry.RowData rowData : rowDatasList) {
                JSONObject jsonObject = new JSONObject();
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                for (CanalEntry.Column column : afterColumnsList) {
                    System.out.println(column.getName() + ":::::::" + column.getValue());
                    String property = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    jsonObject.put(property, column.getValue());
                }

                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER,jsonObject.toJSONString());

            }
        }

    }
}
