package com.lubinsu.hbase

/**
  * Created by lubinsu on 2016/6/7.
  */

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


class HBaseClient(config: Configuration = HBaseConfiguration.create(), tablePath: String,
                  limit: Option[Int] = None, verboseMode: Boolean = false, caching: Int = 5000) extends Serializable  {

  val connection = HbaseUtil.getHbaseConn
  val admin = connection.getAdmin
  val tableName = TableName.valueOf(tablePath)
  val table = connection.getTable(tableName)
  val columnFamilies = table.getTableDescriptor.getColumnFamilies.map(p => p.getNameAsString)

  def getPut(rowkey: String): Put = {
    new Put(Bytes.toBytes(rowkey))
  }

  def getPut(rowkey: String, cf: String, c: String, value: String): Put = {
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(c), Bytes.toBytes(value))
  }

  def getScan: Scan = {
    val scan = new Scan
    //scan.setCaching(caching)
    columnFamilies.foreach(fam => scan.addFamily(Bytes.toBytes(fam)))
    scan
  }

  def getGet(rowKey: String): Get = {
    val get = new Get(Bytes.toBytes(rowKey))
    columnFamilies.foreach(fam => get.addFamily(Bytes.toBytes(fam)))
    get
  }

  def getGet(rowKey: String, filter: Filter): Get = {
    val get = new Get(Bytes.toBytes(rowKey)).setFilter(filter)
    columnFamilies.foreach(fam => get.addFamily(Bytes.toBytes(fam)))
    get
  }

  val _htable = connection.getTable(TableName.valueOf(tablePath))

  def getHtable = _htable

  def readAll: List[Result] = {
    val scan = new Scan

    val htable = getHtable

    val scanner = htable.getScanner(scan)

    val scalaList = scanner.toList

    scanner.close()
    scalaList
  }

  def printVerboseQueryInfo(scan: Scan): Unit = {
    val map = familyMap(scan.getFamilyMap).mkString(",")
  }

  def familyMap(map: java.util.Map[Array[Byte], java.util.NavigableSet[Array[Byte]]]) = {
    map.toList.flatMap {
      case (fam, cols) =>
        val famStr = Bytes.toString(fam)
        if (cols != null) {
          cols map (col => (famStr, Bytes.toString(col)))
        } else {
          List(famStr, "ANY COLUMN")
        }
    }
  }

  def printVerboseQueryInfo(get: Get): Unit = {
    val map = familyMap(get.getFamilyMap).mkString(",")
  }

  def get(get: Get): Result = {
    if (verboseMode) printVerboseQueryInfo(get)
    getHtable.get(get)
  }

  def getBatch(gets: List[Get]): Array[Result] = {
    getHtable.get(gets)
  }

  /* GET */

  def get(rowKey: String, filter: Filter): Result = {
    val getOp = getGet(rowKey)
    getOp.setFilter(filter)
    get(getOp)
  }

  def get(rowKey: String, fam: String, columns: Iterable[String]): Result = {
    val getOp = getGet(rowKey)
    columns.foreach(col => getOp.addColumn(Bytes.toBytes(fam), Bytes.toBytes(col)))
    get(getOp)
  }

  def get(rowKey: String): Result = {
    Try {
      get(getGet(rowKey))
    } match {
      case Success(r) => r
      case Failure(e) => null
    }
  }

  /**
    * 批量获取
    *
    * @param rowKeys rowkey数组
    * @return
    */
  def get(rowKeys: List[String]): Array[Result] = {

    val gets = ListBuffer.empty[Get]

    rowKeys.foreach(r => {
      gets += getGet(r)
    })

    Try {
      getBatch(gets.toList)
    } match {
      case Success(r) => r
      case Failure(e) => null
    }
  }

  /**
    * 批量获取
    *
    * @param rowKeys rowkey数组
    * @return
    */
  def get(rowKeys: List[String], filter: Filter): Array[Result] = {

    val gets = ListBuffer.empty[Get]

    rowKeys.foreach(r => {
      gets += getGet(r).setFilter(filter)
    })

    Try {
      getBatch(gets.toList)
    } match {
      case Success(r) => r
      case Failure(e) => null
    }
  }

  /* Get Value*/
  /**
    * get value from hbase to String
    *
    * @param rowkey key
    * @param cf     column family
    * @param c      column
    * @return value
    */
  def getValue(rowkey: String, cf: String, c: String): String = {
    Try {
      Bytes.toString(get(rowkey).getValue(Bytes.toBytes(cf), Bytes.toBytes(c)))
    } match {
      case Success(value) => value
      case Failure(e) => null
    }
  }

  /* SCAN */

  /**
    * Scan the entire table. When the size of the table is big,
    * it can take a lot of time to complete.
    */
  def scan(r: (Result => Any)) {
    execute(getScan, (res, _) => r(res))
  }

  /**
    * Scan the entire table applying the server-side filter specified.
    * The filter can stop the scan at any point matching the stop criterion.
    */
  def scan(filter: Filter, r: (Result => Any)) {
    val scan = getScan
    scan.setFilter(filter)
    execute(scan, (res, _) => r(res))
  }

  /**
    * Scan the table starting from the specified start row and with the specified
    * filter.
    */
  def scan(startRow: String, filter: Filter, r: (Result => Any)) {
    val scan = getScan
    scan.setFilter(filter)
    scan.setStartRow(Bytes.toBytes(startRow))
    execute(scan, (res, _) => r(res))
  }

  /**
    * Scan the table starting from the specified start row and with the specified
    * filter and the specified columns.
    */
  def scan(startRow: String, filter: Filter, family: String, columns: Iterable[String], r: (Result => Any)) {
    val scan = getScan
    scan.setFilter(filter)
    scan.setStartRow(Bytes.toBytes(startRow))
    columns.foreach(col => scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(col)))
    execute(scan, (res, _) => r(res))
  }

  /**
    * Scan the table only at the specified prefix range with the specified filter and columns.
    */
  def scanPrefix(prefix: String, filter: Filter, family: String, columns: Iterable[String], r: (Result => Any)) {
    val prefixFilter = HBaseFilters.getRowkeyPrefixFilter(prefix)
    scan(prefix, HBaseFilters.combineMustPassAll(List(prefixFilter, filter)), family, columns, r)
  }

  /**
    * Scan the table only at the specified prefix range with the specified filter.
    */
  def scanPrefix(prefix: String, filter: Filter, r: (Result => Any)) {
    val prefixFilter = HBaseFilters.getRowkeyPrefixFilter(prefix)
    scan(prefix, HBaseFilters.combineMustPassAll(List(prefixFilter, filter)), r)
  }

  /**
    * Scan the table only at the specified prefix range and the specified columns.
    */
  def scanPrefix(prefix: String, family: String, columns: Iterable[String], r: (Result => Any)) {
    scan(prefix, HBaseFilters.getRowkeyPrefixFilter(prefix), family, columns, r)
  }

  /**
    * Scan the table only at the specified prefix range.
    */
  def scanPrefix(prefix: String, r: (Result => Any)) {
    scan(prefix, HBaseFilters.getRowkeyPrefixFilter(prefix), r)
  }

  /**
    * Scan the table only at the specified prefix range.
    */
  def scanPrefixOnlyRowKeys(prefix: String, r: (Result => Any)) {
    scanPrefix(prefix, HBaseFilters.getOnlyRowKeyFilter, r)
  }

  def checkIfPrefixExists(prefix: String): Boolean = {
    var exists = false
    scanPrefix(prefix, HBaseFilters.getPageFilter(1), (res: Result) => {
      exists = !res.isEmpty
    })
    exists
  }

  /* DELETE */

  /**
    * 按列删除
    *
    * @param rowKey  行键
    * @param cf      列族
    * @param columns 列
    */
  def deleteColumns(rowKey: String, cf: String, columns: Iterable[String]): Unit = {
    val del = new Delete(Bytes.toBytes(rowKey))
    columns.foreach(col => {
      del.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col))
    })
    table.delete(del)
  }

  /**
    * delete by rowkey
    *
    * @param rowKey rowkey to be deleted
    */
  def deleteRows(rowKey: String) {
    val del = new Delete(Bytes.toBytes(rowKey))
    table.delete(del)
  }

  def deleteRows(filters: FilterList) {
    deleteRows(filters, (_) => true)
  }

  def deleteRows(filters: FilterList, f: (Result => Boolean)) {

    def r = (res: Result, htable: Table) => {
      if (f(res)) {
        val row = res.getRow
        htable.delete(new Delete(row))
      }
    }

    val scan = getScan
    scan.setFilter(filters)

    execute(scan, r)
  }

  /* INSERT BATCH*/
  /**
    * 批量插入表数据
    *
    * @param list 列表数据
    * @throws IOException 抛出异常
    */
  def insert(list: List[Put]) {
    table.put(list)
  }

  /* INSERT */
  /**
    * 插入表数据
    *
    * @param put 列表数据
    * @throws IOException 抛出异常
    */
  def insert(put: Put) {
    table.put(put)
  }

  /* INSERT */

  def insert(rowKey: String, columnGroup: String, columnName: String, columnValue: Array[Byte]): Unit = {
    val htable = getHtable
    val row1 = Bytes.toBytes(rowKey)
    val p1 = new Put(row1)

    p1.addColumn(Bytes.toBytes(columnGroup), Bytes.toBytes(columnName), columnValue)
    htable.put(p1)
    //admin.flush(TableName.valueOf(tablePath))
  }

  /* INSERT */

  def insert(rowKey: String, columnGroup: String, columnName: String, columnValue: String): Unit = {
    val htable = getHtable
    val row1 = Bytes.toBytes(rowKey)
    val p1 = new Put(row1)
    //    p1.setDurability(durability)
    //    p1.setWriteToWAL(false)

    p1.addColumn(Bytes.toBytes(columnGroup), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
    htable.put(p1)
    //admin.flush(TableName.valueOf(tablePath))
  }

  def flush(): Unit = {
    admin.flush(TableName.valueOf(tablePath))
  }

  /* execute method */

  def execute(scan: Scan, r: ((Result, Table) => Any)) {
    val htable = getHtable
    val bigN = 10000

    val scanner = htable.getScanner(scan)
    var i = 0

    val results: Traversable[Result] = limit match {
      case Some(nb) => scanner.next(nb)
      case None => scanner
    }

    val resultFunction = if (verboseMode) {
      printVerboseQueryInfo(scan)
      (res: Result, table: Table) => {
        r(res, table)
      }
    } else r

    if (results != null) {
      results.foreach { res =>
        require(res != null, "Null result returned from HBase scan")
        i += 1
        if (i % bigN == 0) {
        }
        resultFunction(res, htable)
      }
      if (i > bigN) {
      }
    }
    scanner.close()
  }

  def free(): Unit = {
    table.close()
    admin.close()
    connection.close()
  }

  def freeta(): Unit = {
    table.close()
    admin.close()
  }
}

object HBaseClient {
  def getRowKey(res: Result): String = Bytes.toString(res.getRow)

  def getMap(res: Result, family: String): Map[String, String] = {
    res.getFamilyMap(Bytes.toBytes(family)) match {
      case null => Map.empty
      case x: java.util.Map[Array[Byte], Array[Byte]] => x.map {
        case (k, v) => (Bytes.toString(k), Bytes.toString(v))
      }.toMap
    }
  }

  def getColumn(res: Result, family: String, column: String): String =
    Bytes.toString(CellUtil.cloneValue(res.getColumnLatestCell(Bytes.toBytes(family), Bytes.toBytes(column))))

  def getKeywords(res: Result, regex: String): Array[String] =
    Bytes.toString(res.getRow).split(regex)
}
