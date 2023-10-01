package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.{DataFrame, SaveMode}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object RelativePopularityByAggregationIntervalAndTagStorage extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("pathToOutput")
        .flatMap { pathToOutput =>
          c.get[DataFrame]("relativePopularityByAggregationIntervalAndTag").map {
            relativePopularityByAggregationIntervalAndTag =>
              import relativePopularityByAggregationIntervalAndTag.sparkSession.implicits._

              logger.warn("Dumping relative popularity results.")
              relativePopularityByAggregationIntervalAndTag.na
                .fill(0)
                .select(
                  $"start",
                  $"end",
                  $"tag",
                  $"q__entries_count_for_aggregation_interval_and_tag",
                  $"a__entries_count_for_aggregation_interval_and_tag",
                  $"c__entries_count_for_aggregation_interval_and_tag",
                  $"v__entries_count_for_aggregation_interval_and_tag",
                  $"ph__entries_count_for_aggregation_interval_and_tag",
                  $"pl__entries_count_for_aggregation_interval_and_tag",
                  $"q__entries_count_for_aggregation_interval",
                  $"q_a__entries_count_for_aggregation_interval",
                  $"q_a_c__entries_count_for_aggregation_interval",
                  $"q_a_c_v__entries_count_for_aggregation_interval",
                  $"q_a_c_v_ph__entries_count_for_aggregation_interval",
                  $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval",
                  $"q__share",
                  $"q_a__share",
                  $"q_a_c__share",
                  $"q_a_c_v__share",
                  $"q_a_c_v_ph__share",
                  $"q_a_c_v_ph_pl__share"
                )
                .orderBy($"start".asc, $"tag".asc)
                .coalesce(1)
                .write
                .format("csv")
                .option("header", "true")
                .partitionBy("tag")
                .mode(SaveMode.Append)
                .save(pathToOutput)
          }
        }
        .map("RelativePopularityByAggregationIntervalAndTagStorage")(identity)
    }
}
