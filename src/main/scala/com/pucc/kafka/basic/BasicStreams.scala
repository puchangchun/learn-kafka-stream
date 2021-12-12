package com.pucc.kafka.basic

import java.time.Duration
import java.util.Properties

import com.pucc.kafka.common.Common
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder

object BasicStreams extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._


  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-scala-example")
    val bootstrapServers = if (args.length > 0) args(0) else Common.BOOTSTRAP_SERVERS_CONFIG
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder()

  // todo: 指定topic创建Stream
  // 这里其实需要指定Serdes，Scala的隐式转换做了这个工作
  val firstStream = builder.stream[Long, String](topic = "from-basic-topic")

  // todo: 使用一些基础的操作
  firstStream
    .peek((k, v) => println(s"($k : $v)"))  // first record into the Topology
    .filter((k,v)=>v.contains("orderNumber-")) // 取出需要的记录
    .mapValues(v => v.substring(v.indexOf('-') + 1)) // 取出v中的数字部分
    .filter((k, v) => v.toLong > 1000) // 取出大于1000的数字
    .peek((k, v) => println(s"($k : $v)")) // 查看一下过滤后的记录
    .map((k,v)=>("key",v))
    .to(topic = "to-basic-topic") // 写入指定的主题

  // todo: 创建拓扑图，开启应用
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  streams.start()


}
