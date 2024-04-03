package com.wolt.osm.spark.OsmSource

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

class OsmPartition(val input: String, val hadoop: SerializableHadoopConfigration, val schema: StructType, val threads: Int, val partitionsNo: Int, val partition: Int, val useLocal: Boolean) extends InputPartition

object OsmPartition {
  def unapply(inputPartition: OsmPartition): Option[(String, SerializableHadoopConfigration, StructType, Int, Int, Int, Boolean)] = {
    Some((inputPartition.input, inputPartition.hadoop, inputPartition.schema, inputPartition.threads, inputPartition.partitionsNo, inputPartition.partition, inputPartition.useLocal))
  }
}
