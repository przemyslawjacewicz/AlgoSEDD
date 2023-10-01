package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object EntriesCountByAggregationIntervalAndTag extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("entriesCountByCreationDateAndTag")
        .flatMap { entriesCountByCreationDateAndTag =>
          c.get[String]("aggregationInterval").map { aggregationInterval =>
            logger.warn("Aggregating entries by aggregation interval and tag.")
            entriesCountByCreationDateAndTag
              .groupBy(window(col("creation_date"), aggregationInterval).as("aggregation_interval"), col("tag"))
              .agg(
                sum("q__entries_count_for_day_and_tag").as("q__entries_count_for_aggregation_interval_and_tag"),
                sum("a__entries_count_for_day_and_tag").as("a__entries_count_for_aggregation_interval_and_tag"),
                sum("c__entries_count_for_day_and_tag").as("c__entries_count_for_aggregation_interval_and_tag"),
                sum("v__entries_count_for_day_and_tag").as("v__entries_count_for_aggregation_interval_and_tag"),
                sum("ph__entries_count_for_day_and_tag").as("ph__entries_count_for_aggregation_interval_and_tag"),
                sum("pl__entries_count_for_day_and_tag").as("pl__entries_count_for_aggregation_interval_and_tag")
              )
          }
        }
        .map("entriesCountByAggregationIntervalAndTag")(identity)
    }
}
