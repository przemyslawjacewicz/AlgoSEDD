package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, RowFactory, SparkSession}
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.CatalogTransformationWithImplicitCatalog

import java.sql.Date
import java.time._
import pl.epsilondeltalimit.dep.transformation.implicits._

object Days extends (Catalog => Result[DataFrame]) {
  def a(implicit c: Catalog): Catalog =
    c.put {
      c.get[SparkSession]("spark")
        .flatMap { spark =>
          c.get[LocalDate]("startDate").flatMap { startDate =>
            c.get[LocalDate]("endDate").map { endDate =>
              val daysBetween = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays
              spark.createDataFrame(
                spark.sparkContext.parallelize(
                  for (day <- 0L to daysBetween) yield RowFactory.create(Date.valueOf(startDate.plusDays(day)))),
                StructType(Array(StructField("creation_date", DataTypes.DateType)))
              )

              //              (for (day <- 0L to daysBetween) yield LocalDate.parse(startDate, fmt).plusDays(day)).toDF("creation_date")

            }
          }
        }
        .as("days")
    }

  override def apply(implicit c: Catalog): Result[DataFrame] = {
    val xx = {
      for {
        spark <- "spark".as[SparkSession]
        startDate <- "startDate".as[LocalDate]
        endDate <- "endDate".as[LocalDate]
      } yield {
        val daysBetween = Duration.between(startDate.atStartOfDay(), endDate.atStartOfDay()).toDays

        spark.createDataFrame(
          spark.sparkContext.parallelize(
            for (day <- 0L to daysBetween) yield RowFactory.create(Date.valueOf(startDate.plusDays(day)))),
          StructType(Array(StructField("creation_date", DataTypes.DateType)))
        )

        //              (for (day <- 0L to daysBetween) yield LocalDate.parse(startDate, fmt).plusDays(day)).toDF("creation_date")
      }
    }
//      .as("days")

    xx
  }


}
