package com.lubinsu.util.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}

import scala.collection.immutable.HashMap
import scala.reflect.ClassTag

/**
  * Created by lubin 2017/10/19
  */
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {

    private val kc = new KafkaCluster(kafkaParams)

    /**
      * 创建数据流
      *
      * @param ssc 上下文
      * @param kafkaParams 参数
      * @param topics 主题
      * @return
      */
    def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](ssc: StreamingContext, kafkaParams: HashMap[String, String], topics: Set[String]): InputDStream[(K, V)] = {
        val groupId = kafkaParams("group.id")
        //从zookeeper上读取offset开始消费message
        //    val messages = {
        val partitionsE = kc.getPartitions(topics)
        if (partitionsE.isLeft)
            throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
        val partitions = partitionsE.right.get
        val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
        if (!consumerOffsetsE.isLeft) {
            val consumerOffsets = consumerOffsetsE.right.get
            KafkaUtils.createDirectStream(ssc, kafkaParams, consumerOffsets,  (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
        } else {
            val p = kafkaParams + ("auto.offset.reset" -> "largest")
            KafkaUtils.createDirectStream(ssc, p, topics)
        }
        //    }
        //      messages
    }


    /**
      * 更新zookeeper上的消费offsets
      *
      */
    def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
        val groupId = kafkaParams("group.id")
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        for (offsets <- offsetsList) {
            val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
            val o = kc.setConsumerOffsets(groupId, HashMap((topicAndPartition, offsets.untilOffset)))
            if (o.isLeft) {
                println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
            }
        }
    }
    /**
      * 读取zookeeper上的offsets
      *
      */
    def printZKOffsets(rdd: RDD[(String, String)]): Unit = {
        val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        for (offsets <- offsetsList) {
            println("================partition: " + offsets.partition + "\t fromOffset:" + offsets.fromOffset + "\t untilOffset:" + offsets.untilOffset)
        }
    }
}
