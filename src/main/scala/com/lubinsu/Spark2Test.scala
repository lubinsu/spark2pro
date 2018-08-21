package com.lubinsu

import java.util.Properties

import com.lubinsu.util.kafka.{KafkaManager, KafkaSink}
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

/**
  * Created by lubin 2017/7/11
  */
object Spark2Test {

    def main(args: Array[String]): Unit = {

        val kafkaParams = HashMap[String, String]("metadata.broker.list" -> "kafka01:9092,kafka02:9092,kafka03:9092",
            "fetch.message.max.bytes" -> "10485760",
            "num.consumer.fetchers" -> "10",
            "zookeeper.session.timeout.ms" -> "400",
            "zookeeper.connection.timeout.ms" -> "10000",
            "zookeeper.connect" -> "hadoop04,hadoop08,hadoop06,hadoop07,hadoop05",
            "zookeeper.sync.time.ms" -> "200",
            "group.id" -> "test",
            "auto.commit.interval.ms" -> "1000",
            "rebalance.backoff.ms" -> "2000",
            "rebalance.max.retries" -> "10",
            "auto.offset.reset" -> "largest"
        )

        val topics = Array("hy_userinfo").toSet
        val SLIDE_INTERVAL = Seconds(5)

        val spark = SparkSession.builder.
          appName("StructuredNetworkWordCount").
          getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext, SLIDE_INTERVAL)

        // 广播KafkaSink
        val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
            val kafkaProducerConfig = {
                val p = new Properties()
                p.setProperty("bootstrap.servers", kafkaParams.getOrElse("metadata.broker.list", "kafka01:9092,kafka02:9092,kafka03:9092"))
                p.setProperty("key.serializer", classOf[StringSerializer].getName)
                p.setProperty("value.serializer", classOf[StringSerializer].getName)
                p
            }
            ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
        }

        val km = new KafkaManager(kafkaParams)
        val stream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

        stream.foreachRDD(rdd => {
            km.printZKOffsets(rdd)
            rdd.foreachPartition(p => {
                p.foreach(row => {
                    println(row._2)
                    kafkaProducer.value.send("my_streaming_log", row._1, row._1.concat(":").concat(row._2))
                })
            })
            km.updateZKOffsets(rdd)
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
