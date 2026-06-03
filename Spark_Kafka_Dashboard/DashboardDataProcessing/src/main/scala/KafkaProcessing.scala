import java.util.Properties

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * Created by 刘煜澄 on 2020/4/8. 
 * Descritpion ${DESCRIPTION}
 */
object KafkaProcessing {
  def main(args: Array[String]): Unit = {
    var setting_path: String=""
    if(args.length<1){
      setting_path = "/Users/liuyudeng/Desktop/BigDataUnionpay/unionpay_dashboard/python_dashboard/setting.py"
    }else{
      setting_path = args(0)
    }

    val props = readFromTxtByLine(setting_path)
    //如果生产消费者名字一样则不用scala处理数据
    if(props("PRODUCER_TOPIC")==props("CONSUMER_TOPIC")){
      return
    }
    val conf = new SparkConf()
      .setAppName(props("SPARK_APP"))
      .setMaster(props("SPARK_HOST"))
    val ssc = new StreamingContext(conf, Seconds(props("GENERATE_INTERVAL").toInt))

    val topicsSet = Array(props("PRODUCER_TOPIC"))
    val kafkaParams = mutable.HashMap[String, String]()
    //必须添加以下参数，否则会报错
    kafkaParams.put("bootstrap.servers", props("KAFKA_ADDRESS"))
    kafkaParams.put("group.id", props("GROUP_ID"))
    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams
      )
    )

    val MAPPER = new ObjectMapper()

    val lines = messages.map(_.value)
    lines.print()
    //转为RDD[性别，城市，消费类型，支付方式，金额]
    val records = lines.flatMap(_.split("}, \\{")).map(t => {
      val kvs = t.split(",")
      (kvs(1).split(":")(1).replace("\"", "").trim,
        kvs(3).split(":")(1).replace("\"", "").trim,
        kvs(4).split(":")(1).replace("\"", "").trim,
        kvs(6).split(":")(1).replace("\"", "").trim,
        kvs(5).split(":")(1).replace("\"", "").trim)
    })

    records.foreachRDD(record => {
      //把所有数据都放进一个list里，1-4城市，5-6性别，7-10支付方式，11-16消费类型
      val v1: Double = record.filter(t => {
        t._2.equals("北京")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v2: Double = record.filter(t => {
        t._2.equals("上海")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v3: Double = record.filter(t => {
        t._2.equals("广州")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v4: Double = record.filter(t => {
        t._2.equals("深圳")
      }).map(x => {
        (x._5.toInt)
      }).sum()

      val v5: Double = record.filter(t => {
        t._1.equals("男")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v6: Double = record.filter(t => {
        t._1.equals("女")
      }).map(x => {
        (x._5.toInt)
      }).sum()

      val v7: Double = record.filter(t => {
        t._4.equals("银联")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v8: Double = record.filter(t => {
        t._4.equals("支付宝")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v9: Double = record.filter(t => {
        t._4.equals("微信")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v10: Double = record.filter(t => {
        t._4.equals("现金")
      }).map(x => {
        (x._5.toInt)
      }).sum()

      val v11: Double = record.filter(t => {
        t._3.equals("交通")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v12: Double = record.filter(t => {
        t._3.equals("餐饮")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v13: Double = record.filter(t => {
        t._3.equals("娱乐")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v14: Double = record.filter(t => {
        t._3.equals("教育")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v15: Double = record.filter(t => {
        t._3.equals("住房")
      }).map(x => {
        (x._5.toInt)
      }).sum()
      val v16: Double = record.filter(t => {
        t._3.equals("其他")
      }).map(x => {
        (x._5.toInt)
      }).sum()

      var result = Array(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16)

      val kafkaProps = new Properties()
      kafkaProps.put("bootstrap.servers", props("KAFKA_ADDRESS"))
      kafkaProps.put("group.id", props("GROUP_ID"))
      kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      kafkaProps.put("key.serializer", classOf[StringSerializer].getName)
      kafkaProps.put("value.serializer", classOf[StringSerializer].getName)
      // 实例化一个Kafka生产者
      val producer = new KafkaProducer[String, String](kafkaProps)
      val message = MAPPER.writeValueAsString(result)
      // 封装成Kafka消息
      val msg = new ProducerRecord[String, String](props("CONSUMER_TOPIC"), message)
      // 给Kafka发送消息
      producer.send(msg, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (metadata != null) {
            println("发送成功")
          }
          if (exception != null) {
            println("消息发送失败")
          }
        }
      })
      producer.close()
    }
    )

    ssc.start()
    ssc.awaitTermination()

  }

  //读取本地配置文件
  def readFromTxtByLine(filePath: String) = {
    //导入Scala的IO包
    import scala.io.Source
    //以指定的UTF-8字符集读取文件，第一个参数可以是字符串或者是java.io.File
    //val source = Source.fromFile("/Users/liuyudeng/Desktop/PyProjects/unionpay_dashboard/setting.py", "UTF-8")
    val source = Source.fromFile(filePath, "UTF-8")
    //或取文件中所有行
    //val lineIterator = source.getLines()
    //迭代打印所有行
    //lineIterator.foreach(println)
    var result: Map[String, String] = Map()
    //将所有行放到数组中
    val lines = source.getLines()
    lines.foreach(t => {
      if (t.contains("KAFKA_ADDRESS")) {
        result += ("KAFKA_ADDRESS" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("GENERATE_INTERVAL")) {
        result += ("GENERATE_INTERVAL" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("PRODUCER_TOPIC")) {
        result += ("PRODUCER_TOPIC" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("CONSUMER_TOPIC")) {
        result += ("CONSUMER_TOPIC" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("SPARK_HOST")) {
        result += ("SPARK_HOST" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("SPARK_APP")) {
        result += ("SPARK_APP" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
      if (t.contains("GROUP_ID")) {
        result += ("GROUP_ID" -> t.split("=")(1).trim.replaceAll("'", ""))
      }
    })
    source.close()
    //println(lines.size)
    result
  }

}
