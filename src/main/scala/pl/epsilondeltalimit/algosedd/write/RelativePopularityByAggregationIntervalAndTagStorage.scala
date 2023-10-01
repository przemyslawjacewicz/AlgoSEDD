package pl.epsilondeltalimit.algosedd.write

import org.apache.spark.sql.functions.col
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
              logger.warn("Dumping relative popularity results.")
              relativePopularityByAggregationIntervalAndTag.na
                .fill(0)
                .select(
                  col("tag"),
                  col("start"),
                  col("end"),
                  col("q__entries_count_for_aggregation_interval_and_tag"),
                  col("a__entries_count_for_aggregation_interval_and_tag"),
                  col("c__entries_count_for_aggregation_interval_and_tag"),
                  col("v__entries_count_for_aggregation_interval_and_tag"),
                  col("ph__entries_count_for_aggregation_interval_and_tag"),
                  col("pl__entries_count_for_aggregation_interval_and_tag"),
                  col("q__entries_count_for_aggregation_interval"),
                  col("q_a__entries_count_for_aggregation_interval"),
                  col("q_a_c__entries_count_for_aggregation_interval"),
                  col("q_a_c_v__entries_count_for_aggregation_interval"),
                  col("q_a_c_v_ph__entries_count_for_aggregation_interval"),
                  col("q_a_c_v_ph_pl__entries_count_for_aggregation_interval"),
                  col("q__share"),
                  col("q_a__share"),
                  col("q_a_c__share"),
                  col("q_a_c_v__share"),
                  col("q_a_c_v_ph__share"),
                  col("q_a_c_v_ph_pl__share")
                )
                .orderBy(col("start").asc, col("tag").asc)
                .coalesce(1)
                .write
                .format("csv")
                .option("header", "true")
                .partitionBy("tag")
                .mode(SaveMode.Append)
                .save(pathToOutput)
          }
        }
        .map("relativePopularityByAggregationIntervalAndTagStorage")(identity)
    }
}
