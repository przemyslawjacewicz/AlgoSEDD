package pl.epsilondeltalimit.analyzer.support.storage

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class XmlFileStorage(spark: SparkSession) {

  def readFromXmlFile(rowTag: String, path: String): DataFrame = {
    spark.read
      .format("xml")
      .option("rowTag", rowTag)
      .load(path)
  }

  def readFromXmlFile(rowTag: String, schema: StructType, path: String): DataFrame = {
    spark.read
      .format("xml")
      .option("rowTag", rowTag) //"row"
      .option("inferSchema", value = false)
      .schema(schema)
      .load(path)
  }

}
