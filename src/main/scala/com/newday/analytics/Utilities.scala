package com.newday.analytics

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

trait Utilities {

  /**
   * file schema generator
   */
  def getSchema(fileName: String): StructType = {
    val json_schema_string = Source.fromInputStream(getClass.getResourceAsStream(s"/schema/${fileName}.json")).mkString
    DataType.fromJson(json_schema_string) match {
      case s: StructType => s
    }
  }

  /**
   * csv reader
   */
  def csvReader(spark: SparkSession, inputPath: String, fileName: String): DataFrame = {
    spark.read.option("delimiter", "::").schema(getSchema(fileName)).csv(s"${inputPath}/${fileName}.dat")
  }

  def writeDF(outputDF: DataFrame, outputPath: String, resultSetName: String) = {
    // to reduce number of output part files
    val numPartitions = outputDF.sparkSession.sparkContext.defaultParallelism
    outputDF.repartition(numPartitions).write.mode(SaveMode.Overwrite).parquet(s"${outputPath}/${resultSetName}/")
  }

}
