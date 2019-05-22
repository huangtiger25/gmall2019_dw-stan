package com.atguigu.gmall2019.dw.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.dw.constant.GmallConstant
import com.atguigu.gmall.dw.util.MyEsUtil
import com.atguigu.gmall2019.dw.realtime.bean.StartUpLog
import com.atguigu.gmall2019.dw.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dag_app").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(5))

    val inputDStream = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

    //0 转换格式  把json换成样例类  补充时间格式
    val startUpLogDStream = inputDStream.map { record =>
      val jsonString = record.value()
      val startUpLog = JSON.parseObject(jsonString, classOf[StartUpLog])
      val datetimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startUpLog.ts))
      val datetimeArr = datetimeStr.split(" ")
      startUpLog.logDate = datetimeArr(0)
      startUpLog.logHourMinute = datetimeArr(1)
      startUpLog.logHour = datetimeArr(1).split(":")(0)
      startUpLog
    }


    //1.根据redis的访问记录过滤今日已经活跃过的用户
    val filteredStartUpLogDStream = startUpLogDStream.transform{ rdd =>
      val jedis = RedisUtil.getJedisClient

      val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val jedisKey = "dau:" + date
      val dauSet = jedis.smembers(jedisKey)
      jedis.close()
      val dauBC = ssc.sparkContext.broadcast(dauSet)

      val filteredRDD = rdd.filter { startuplog =>
        !dauBC.value.contains(startuplog.mid)
      }
      filteredRDD

    }

    //2.过滤后DStream进行一次去重，以mid进行分组，每组取一个
    val groupByMidDStream = filteredStartUpLogDStream.map(startupLog => (startupLog.mid,startupLog)).groupByKey()
    val distinctStartUpLogDStream = groupByMidDStream.flatMap{case (mid,item) => item.take(1)}


    //3.记录今日活跃过的用户
    //保存redis value类型 set     key ：   dau：2019-xx-xx  values:mid
    distinctStartUpLogDStream.foreachRDD{rdd =>

      rdd.foreachPartition{ startupLogItem =>

        val jedis = RedisUtil.getJedisClient


        val starupList = startupLogItem.toList
        for (starupLog <- starupList) {
          val key = "dau:" + starupLog.logDate
          jedis.sadd(key,starupLog.mid)
        }

        println(startupLogItem.length)
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_DAU,starupList)

        jedis.close()
      }

    }






    ssc.start()

    ssc.awaitTermination()
  }
}
