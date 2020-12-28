package pl.epsilondeltalimit.analyzer.analyze

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, count, explode, isnull, lit, not, size}

object AnalyzeSupport {

  val postRecordIsQuestion: Column = col("post_type_id") === 1 && not(isnull(col("tags")))

  val postRecordIsAnswer: Column = col("post_type_id") === 2 && not(isnull(col("parent_id")))

  def countEntriesByCreationDateAndTag(tagsByCreationDate: DataFrame): DataFrame = tagsByCreationDate
    .select(col("creation_date"), explode(col("tags")).as("tag"))
    .where(not(isnull(col("tags"))))
    .groupBy("creation_date", "tag")
    .count()
}
