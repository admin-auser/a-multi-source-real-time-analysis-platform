package org.apache.spark.examples.streaming

import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark._
import java.io._
import scala.util.{Success, Try}
import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Interval
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaDataAll {
  implicit val formats = DefaultFormats//数据格式化时需要
  def main(args: Array[String]): Unit={
    if (args.length < 4) {
      System.err.println("Usage: KafkaDataAll <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    StreamingExamples.setStreamingLogLevels()
    /* 输入的四个参数分别代表着
    * 1. zkQuorum 为zookeeper地址
    * 2. group为消费者所在的组
    * 3. topics该消费者所消费的topics
    * 4. numThreads开启消费topic线程的个数
    */
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaDataAll").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint(".")  //这里表示把检查点文件写入分布式文件系统HDFS，所以要启动Hadoop
    // 将topics转换成topic-->numThreads的哈稀表
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // 创建连接Kafka的消费者链接
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //val flines = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 17)).map(_.split(",")(4))
    val flines = lines.filter(line => (line.trim().length > 0) && (line.split(",").length == 17) && (line.split(",")(3).length > 10) && (line.split(",")(3).startsWith("2022")))
    val f1 = flines.filter(line => (line.split(",")(4).startsWith("Ⅰ") || line.split(",")(4).startsWith("Ⅱ") || line.split(",")(4).startsWith("Ⅲ")))
    val f2 = flines.filter(line => (line.split(",")(4).startsWith("Ⅳ") || line.split(",")(4).startsWith("Ⅴ") || line.split(",")(4).startsWith("劣Ⅴ")))
    /*val goodtmp = f1.map(x=>{
           (x.split(",")(3).substring(5,10),x.split(",")(4))
    })
    val good = goodtmp.map(x=>x._1+"优")
    val poortmp = f2.map(x=>{
           (x.split(",")(3).substring(5,10),x.split(",")(4))
      })
    val poor = poortmp.map(x=>x._1+"劣")*/
    val good = f1.map(x=>{
           x.split(",")(3).substring(5,10)+",优"
    })
    val poor = f2.map(x=>{
           x.split(",")(3).substring(5,10)+",劣"
      })
    val total = good.union(poor)
    val words = total.flatMap(_.split(" "))//将输入的每行用空格分割成一个个word
    //val words = flines.flatMap(_.split(" "))//将输入的每行用空格分割成一个个word
    // 对每一秒的输入数据进行reduce，然后将reduce后的数据发送给Kafka
    val wordCounts = words.map(x => (x, 1))
    //val wordCounts = total.map(x => (x, 1))
    val stateDstream = wordCounts.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    var strtmp = ""
    stateDstream.foreachRDD(rdd => {
          if(rdd.count !=0 ){
              /* val props = new HashMap[String, Object]()
               props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               // 实例化一个Kafka生产者
               val producer = new KafkaProducer[String, String](props)*/
               // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
               //val str = write(rdd.collect)
               val str = rdd.groupByKey().sortByKey(true).collect().map(r => (r._1,r._2.toList.sortWith(_<_)).toString().replaceAll("List","").replaceAll("\\(","").replaceAll("\\)","")).mkString("\n")
               //if(!str.equals(strtmp)){
               if(str.length != strtmp.length){
                 val props = new HashMap[String, Object]()
               props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               // 实例化一个Kafka生产者
               val producer = new KafkaProducer[String, String](props)

                   // 封装成Kafka消息，topic为"result"
                   val message = new ProducerRecord[String, String]("result", null, str)
                   // 给Kafka发送消息
                   producer.send(message)
                   println("tmp"+strtmp)
                   strtmp = str
                   println("notmp"+str)

               }
               // 封装成Kafka消息，topic为"result"
               //val message = new ProducerRecord[String, String]("result", null, str)
               // 给Kafka发送消息
               //producer.send(message)
          }
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
