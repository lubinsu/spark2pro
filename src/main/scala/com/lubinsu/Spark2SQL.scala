package com.lubinsu

import com.lubinsu.hbase.HBaseClient
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions.explode
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
  * Created by lubin 2017/7/11
  */
case class my_crawler_jd_report(
                                 key: String,
                                 addressdetail: String,
                                 addressname: String,
                                 addressphone: String,
                                 addtime: String,
                                 invoicetitle: String,
                                 invoicetype: String,
                                 orderid: String,
                                 ordertime: String,
                                 userid: String
                               )

object Spark2SQL {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder.
          appName("my_crawler_jd_report").
          getOrCreate()


        def catalog =
            s"""{
               |"table":{"namespace":"default", "name":"my_crawler_jd_order_integration"},
               |"rowkey":"key",
               |"columns":{
               |"key":{"cf":"rowkey", "col":"key", "type":"string"},
               |"addressdetail":{"cf":"order_info", "col":"addressdetail", "type":"string"},
               |"addressname":{"cf":"order_info", "col":"addressname", "type":"string"},
               |"addressphone":{"cf":"order_info", "col":"addressphone", "type":"string"},
               |"addtime":{"cf":"order_info", "col":"addtime", "type":"string"},
               |"invoicetitle":{"cf":"order_info", "col":"invoicetitle", "type":"string"},
               |"invoicetype":{"cf":"order_info", "col":"invoicetype", "type":"string"},
               |"orderid":{"cf":"order_info", "col":"orderid", "type":"string"},
               |"ordertime":{"cf":"order_info", "col":"ordertime", "type":"string"},
               |"userid":{"cf":"order_info", "col":"userid", "type":"string"}
               |}
               |}""".stripMargin

        var date = DateTime.now.toString("yyyyMMdd")
        if (args.length == 1) {
            date = args(0)
        }
        val df = spark.read.json("/user/hadoop/flume/my_crawler_jd_report/".concat(date).concat("/*"))

        val dfOrderInfo = df.select(df("sUserId"), df("addTime"), explode(df("message.orderInfo"))).toDF("userid", "addtime", "orderInfo")

        val rddAddress = dfOrderInfo.select("orderInfo.address"
            , "orderInfo.consignee"
            , "orderInfo.phoneNumber"
            , "addtime"
            , "orderInfo.invoiceTitle"
            , "orderInfo.invoiceType"
            , "orderInfo.orderNo"
            , "orderInfo.shipTime"
            , "userid").toDF("addressdetail", "addressname", "addressphone"
            , "addtime"
            , "invoicetitle"
            , "invoicetype"
            , "orderid"
            , "ordertime"
            , "userid").rdd.mapPartitions(p => {
            val ods_hy_userinfo_sguid = new HBaseClient(tablePath = "ods_hy_userinfo_sguid")
            //            val my_crawler_jd_order_integration = new HBaseClient(tablePath = "my_crawler_jd_order_integration")
            val part = p.map(row => {
                val sidno = ods_hy_userinfo_sguid.getValue(row.getString(8), "yhxx", "sidno")

                //                my_crawler_jd_order_integration.insert(sidno.concat("_MY_").concat(row.getString(6)), "f", "addressdetail", row.getString(0))
                val rpt = my_crawler_jd_report(sidno.concat("_MY_").concat(row.getString(6))
                    , row.getString(0)
                    , row.getString(1)
                    , row.getString(2)
                    , row.getString(3)
                    , row.getString(4)
                    , row.getString(5)
                    , row.getString(6)
                    , row.getString(7)
                    , row.getString(8)
                )
                println(rpt.key)
                rpt
            })

            //            ods_hy_userinfo_sguid.free()
            part
        })
        val dfAddress = spark.createDataFrame(rddAddress)
        dfAddress.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
          .format("org.apache.spark.sql.execution.datasources.hbase").save()

        val x = spark.sparkContext.parallelize(Array(("a", 1), ("b", 1), ("a", 1),
            ("a", 1), ("b", 1), ("b", 1),
            ("b", 1), ("b", 1)), 3)

        x.aggregateByKey(ListBuffer.empty[String])(
            (numList, num) => {
                numList += String.valueOf(num)
                numList
            },
            (numList1, numList2) => numList1 ++ numList2
        )
    }

}
