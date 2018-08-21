package com.lubinsu.hbase

/**
  * Created by lubinsu on 2016/6/7.
  */

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, TableName}

import scala.collection.JavaConversions._


class HBasePool(tablePath: String, threadCount: Int = 1, limit: Option[Int] = None, verboseMode: Boolean = false, caching: Int = 5000) {

  val connection = HbaseUtil(threadCount)
  val admin = connection.getAdmin
  val tableName = TableName.valueOf(tablePath)
  val table = connection.getTable(tableName)
  val columnFamilies = table.getTableDescriptor.getColumnFamilies.map(p => p.getNameAsString)

  def getScan: Scan = {
    val scan = new Scan
    scan.setCaching(caching)
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

  def get(rowKey: String): Result = get(getGet(rowKey))

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

  /* INSERT */

  def insert(rowKey: String, columnGroup: String, columnName: String, columnValue: String): Unit = {
    val htable = getHtable
    val row1 = Bytes.toBytes(rowKey)
    val p1 = new Put(row1)

    p1.addColumn(Bytes.toBytes(columnGroup), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
    htable.put(p1)
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
}

object HBasePool {
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

object HBaseFilters {

  def combineMustPassOne(filters: Iterable[Filter]): Filter =
    new FilterList(FilterList.Operator.MUST_PASS_ONE, filters.toList)

  def combineMustPassAll(filters: Iterable[Filter]): Filter =
    new FilterList(FilterList.Operator.MUST_PASS_ALL, filters.toList)

  def getColumnPrefixesFilter(prefixes: Iterable[String]): Filter =
    combineMustPassOne(prefixes.map(prefix => getColumnPrefixFilter(prefix)))

  def getRowkeyPrefixFilter(prefix: String): Filter = new PrefixFilter(Bytes.toBytes(prefix))

  def getRowKeyRegexFilter(regex: String): Filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(regex))

  def getColumnEqualFilter(family: String, column: String, value: String): Filter = {
    val filter = new SingleColumnValueFilter(Bytes.toBytes(family),
      Bytes.toBytes(column),
      CompareFilter.CompareOp.EQUAL,
      Bytes.toBytes(value))
    filter.setFilterIfMissing(true)
    filter
  }

  def getColumnLargerEqualFilter(family: String, column: String, value: String): Filter = {
    val filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(column), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(value))
    filter.setFilterIfMissing(true)
    filter
  }

  def getColumnRegexFilter(family: String, column: String, regex: String): Filter =
    new SingleColumnValueFilter(Bytes.toBytes(family),
      Bytes.toBytes(column),
      CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(regex))

  def getColumnPrefixFilter(prefix: String): Filter = new ColumnPrefixFilter(Bytes.toBytes(prefix))

  def getOnlyRowKeyFilter: Filter =
    new FilterList(FilterList.Operator.MUST_PASS_ALL, List(new FirstKeyOnlyFilter(), new KeyOnlyFilter()))

  def getPageFilter(n: Int): Filter = new PageFilter(n)
}
