package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object EntriesCountByAggregationInterval extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("entriesCountByAggregationIntervalAndTag").map("entriesCountByAggregationInterval") {
        entriesCountByAggregationIntervalAndTag =>
          logger.warn("Aggregating entries by aggregation interval.")
          entriesCountByAggregationIntervalAndTag
            .groupBy("aggregation_interval")
            .agg(
              sum(col("q__entries_count_for_aggregation_interval_and_tag"))
                .as("q__entries_count_for_aggregation_interval"),
              sum(col("q__entries_count_for_aggregation_interval_and_tag") +
                col("a__entries_count_for_aggregation_interval_and_tag"))
                .as("q_a__entries_count_for_aggregation_interval"),
              sum(
                col("q__entries_count_for_aggregation_interval_and_tag") +
                  col("a__entries_count_for_aggregation_interval_and_tag") +
                  col("c__entries_count_for_aggregation_interval_and_tag"))
                .as("q_a_c__entries_count_for_aggregation_interval"),
              sum(
                col("q__entries_count_for_aggregation_interval_and_tag") +
                  col("a__entries_count_for_aggregation_interval_and_tag") +
                  col("c__entries_count_for_aggregation_interval_and_tag") +
                  col("v__entries_count_for_aggregation_interval_and_tag"))
                .as("q_a_c_v__entries_count_for_aggregation_interval"),
              sum(
                col("q__entries_count_for_aggregation_interval_and_tag") +
                  col("a__entries_count_for_aggregation_interval_and_tag") +
                  col("c__entries_count_for_aggregation_interval_and_tag") +
                  col("v__entries_count_for_aggregation_interval_and_tag") +
                  col("ph__entries_count_for_aggregation_interval_and_tag"))
                .as("q_a_c_v_ph__entries_count_for_aggregation_interval"),
              sum(
                col("q__entries_count_for_aggregation_interval_and_tag") +
                  col("a__entries_count_for_aggregation_interval_and_tag") +
                  col("c__entries_count_for_aggregation_interval_and_tag") +
                  col("v__entries_count_for_aggregation_interval_and_tag") +
                  col("ph__entries_count_for_aggregation_interval_and_tag") +
                  col("pl__entries_count_for_aggregation_interval_and_tag"))
                .as("q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
            )
      }
    }
}
