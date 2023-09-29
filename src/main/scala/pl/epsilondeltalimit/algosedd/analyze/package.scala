package pl.epsilondeltalimit.algosedd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

package object analyze {
  def countEntriesByCreationDateAndTag(tagsByCreationDate: DataFrame): DataFrame = tagsByCreationDate
    .select(col("creation_date"), explode(col("tags")).as("tag"))
    .where(not(isnull(col("tags"))))
    .groupBy("creation_date", "tag")
    .count()

}
