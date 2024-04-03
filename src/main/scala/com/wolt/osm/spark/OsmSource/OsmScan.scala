package com.wolt.osm.spark.OsmSource

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}

import java.io.File
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.util.Try

class OsmScan(val options: CaseInsensitiveStringMap) extends Scan with Batch {
  private val path = options.get("path")
  private val useLocal = Option(options.get("useLocalFile")).getOrElse("").equalsIgnoreCase("true")

  private val spark = SparkSession.active
  private val hadoop = spark.sessionState.newHadoopConf()
  private val hadoopConfigration = new SerializableHadoopConfigration(hadoop)

  if (useLocal) {
    if (!new File(SparkFiles.get(path)).canRead) {
      throw new RuntimeException(s"Input unavailable: $path")
    }
  } else {
    val source = new Path(path)
    val fs = source.getFileSystem(hadoop)
    if (!fs.exists(source)) {
      throw new RuntimeException(s"Input unavailable: $path")
    }
  }

  private val partitions = Option(options.get("partitions")).getOrElse("1")
  private val threads = Option(options.get("threads")).getOrElse("1")

  override def readSchema(): StructType = OsmSource.schema

  override def planInputPartitions(): Array[InputPartition] = {
    val partitionsNo = Try(partitions.toInt).getOrElse(1)
    val threadsNo = Try(threads.toInt).getOrElse(1)
    val shiftedPartitions = partitionsNo - 1
    (0 to shiftedPartitions).map(p => new OsmPartition(path, hadoopConfigration, readSchema(), threadsNo, partitionsNo, p, useLocal)).toArray
  }

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory = new OsmPartitionReaderFactory()
}
