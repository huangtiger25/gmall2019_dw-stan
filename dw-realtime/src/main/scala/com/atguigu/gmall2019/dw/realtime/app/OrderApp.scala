package com.atguigu.gmall2019.dw.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.dw.constant.GmallConstant
import com.atguigu.gmall.dw.util.MyEsUtil
import com.atguigu.gmall2019.dw.realtime.bean.OrderInfo
import com.atguigu.gmall2019.dw.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)

    //转换成orderInfoDStream
    val orderInfoDStream = inputDStream.map { record =>
      val jsonString = record.value()
      val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
      //补充日期
      val dataTimeArr = orderInfo.createTime.split(" ")
      orderInfo.createDate = dataTimeArr(0)
      orderInfo.createHourMinute = dataTimeArr(1)
      orderInfo.createHour = dataTimeArr(1).split(":")(0)

      //手机号脱敏   138****2938
      val tel1 = orderInfo.consigneeTel.splitAt(3)
      val tel2 = orderInfo.consigneeTel.splitAt(7)
      orderInfo.consigneeTel = tel1._1 + "****" + tel2._2

      orderInfo
    }
    // 插入es
    orderInfoDStream.foreachRDD{rdd =>
      rdd.foreachPartition{orderInfoItem =>
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_NEWORDER,orderInfoItem.toList)
      }
    }

    //检查客户是否第一次下单



    //开启
    ssc.start()


    ssc.awaitTermination()

  }
}
