package com.wolt.osm.spark.OsmSource

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

object OsmSource {
  val OSM_SOURCE_NAME = "com.wolt.osm.spark.OsmSource"

  private val info = Seq(
    StructField("UID", IntegerType, nullable = true),
    StructField("USERNAME", StringType, nullable = true),
    StructField("VERSION", IntegerType, nullable = true),
    StructField("TIMESTAMP", LongType, nullable = true),
    StructField("CHANGESET", LongType, nullable = true),
    StructField("VISIBLE", BooleanType, nullable = false)
  )

  private val member = Seq(
    StructField("ID", LongType, nullable = false),
    StructField("ROLE", StringType, nullable = true),
    StructField("TYPE", IntegerType, nullable = false)
  )

  private val fields = Seq(
    StructField("ID", LongType, nullable = false),
    StructField("TAG", MapType(StringType, StringType, valueContainsNull = false), nullable = false),
    StructField("INFO", StructType(info), nullable = true),
    StructField("TYPE", IntegerType, nullable = false),
    StructField("LAT", DoubleType, nullable = true),
    StructField("LON", DoubleType, nullable = true),
    StructField("WAY", ArrayType(LongType, containsNull = false), nullable = true),
    StructField("RELATION", ArrayType(StructType(member), containsNull = false), nullable = true)
  )

  val schema: StructType = StructType(fields)
}

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = OsmSource.schema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = new OsmTable()
}

class OsmTable extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = OsmSource.schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new OsmScanBuilder(options)
}

class OsmScanBuilder(val options: CaseInsensitiveStringMap) extends ScanBuilder {

  override def build(): Scan = new OsmScan(options)
}

class OsmPartitionReaderFactory() extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new OsmPartitionReader(partition.asInstanceOf[OsmPartition])
}
