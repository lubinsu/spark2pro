package com.lubinsu.hbase

import java.io.ByteArrayOutputStream
import java.util.concurrent.Executors

import com.lamfire.utils.PropertiesUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.util.Base64

/**
  * Created by lubinsu on 7/28/2016.
  */
object HbaseUtil extends Serializable /*with ClusterEnv*/ {

  private val conf = HBaseConfiguration.create()

  private val props = PropertiesUtils.load("bigdata.properties", this.getClass)
  conf.set("hbase.zookeeper.quorum", props.getProperty("hbase.zookeeper.quorum"))

  def apply(threadCount: Int = 1): Connection = {
    val executor = Executors.newFixedThreadPool(threadCount)
    ConnectionFactory.createConnection(conf, executor)
  }

  def getConf = conf

  /*val executor = Executors.newFixedThreadPool(10)
  private val connection = ConnectionFactory.createConnection(conf, executor)*/
  def getHbaseConn: Connection = {
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

  def convertScanToString(scan: Scan): String = {
    try {
      val out = new ByteArrayOutputStream()
      Base64.encodeBytes(out.toByteArray);
    } catch {
      case e: Exception => e.printStackTrace()
        ""
    }
  }
}
