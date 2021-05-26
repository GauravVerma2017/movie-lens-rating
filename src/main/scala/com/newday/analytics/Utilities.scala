package com.newday.analytics

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.io.Source

trait Utilities {

  /**
   * getSchema reads json file from resources and returns a struct type schema
   * @param fileName string argument
   * @return struct type schema
   */
  def getSchema(fileName: String): StructType = {
    val json_schema_string = Source.fromInputStream(getClass.getResourceAsStream(s"/schema/$fileName.json")).mkString
    DataType.fromJson(json_schema_string) match {
      case s: StructType => s
    }
  }

  /**
   * csv_Reader reads csv files and retuns spark data frame
   * @param spark SparkSession object
   * @param inputPath file path string
   * @param fileName fileName string
   * @return spark dataframe
   */
  def csvReader(spark: SparkSession, inputPath: String, fileName: String): DataFrame = {
    spark.read.option("delimiter", "::").schema(getSchema(fileName)).csv(s"$inputPath/$fileName.dat")
  }

  /**
   * writeDF functins writes dataframe to output path with overwrite mode
   * @param outputDF result dataframe
   * @param outputPath output path string
   * @param resultSetName tableName or resultset Name string
   */
  def writeDF(outputDF: DataFrame, outputPath: String, resultSetName: String):Unit = {
    // to reduce number of output part files
    val numPartitions = outputDF.sparkSession.sparkContext.defaultParallelism
    outputDF.repartition(numPartitions).write.mode(SaveMode.Overwrite).parquet(s"$outputPath/$resultSetName/")
  }

}
