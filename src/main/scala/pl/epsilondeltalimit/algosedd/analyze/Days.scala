package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.types._
import org.apache.spark.sql.{RowFactory, SparkSession}
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.transformation.implicits._

import java.sql.Date
import java.time._

object Days extends (Catalog => Catalog) {
  override def apply(c: Catalog): Catalog = {
    implicit val _c: Catalog = c

    c.put {
      {
        for {
          spark     <- "spark".as[SparkSession]
          startDate <- "startDate".as[LocalDate]
          endDate   <- "endDate".as[LocalDate]
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
        .as("days")
    }
  }

}
