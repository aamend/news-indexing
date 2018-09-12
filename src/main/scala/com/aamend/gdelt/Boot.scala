package com.aamend.gdelt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.aamend.spark.gdelt._
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.apache.spark.sql.magellan.dsl.expressions._

object Boot {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val inputDir: ScallopOption[String] = opt[String](required = true)
    val outputDir: ScallopOption[String] = opt[String](required = true)
    val shapeFile: ScallopOption[String] = opt[String](required = true)
    val shapeField: ScallopOption[String] = opt[String](required = false, default = Some("lad17cd"))
    val partitions: ScallopOption[Int] = opt[Int](required = false, default = Some(1))
    verify()
  }

  def main(args: Array[String]): Unit = {

    val conf = new Conf(args)

    val spark = SparkSession
      .builder()
      .appName("GDELT_INDEXING")
      .getOrCreate()

    import spark.implicits._

    val inputDir = conf.inputDir.apply()
    val outputDir = conf.outputDir.apply()
    val shapeFile = conf.shapeFile.apply()

    val gcamCodes = spark
      .loadGcams
      .map(r => {
        (r.gcamCode, r.dimensionHumanName)
      })
      .toDF("gcamCode", "gcam")

    val geoshape = spark
      .read
      .format("magellan")
      .load(shapeFile)
      .withColumn("district", col("metadata")(conf.shapeField.apply()))
      .drop("metadata", "point", "polyline")

    val gdelt = spark
      .read
      .gdeltGkg(inputDir)
      .rdd
      .map(gkg => {
        (
          gkg.publishDate.getTime,
          gkg.gcams.filter(_.gcamCode.startsWith("c18")), // only GDELT gcam
          gkg.locations.filter(l => l.countryCode == "UK" && l.geoType == "WORLDCITY"), // only local news
          gkg.tone.tone
        )
      })
      .filter({ case (_, gcams, locations, _) =>
        gcams.nonEmpty && locations.nonEmpty
      })
      .flatMap({ case (ts, gcams, locations, tone) =>
        gcams.map(gcam => {
          (ts, gcam, locations, tone)
        })
      })
      .flatMap({ case (ts, gcam, locations, tone) =>
        locations.map(location => {
          (ts, gcam.gcamCode, gcam.gcamValue, location.geoPoint.latitude, location.geoPoint.longitude, tone)
        })
      })
      .toDF("timestamp", "gcamCode", "wordcount", "lat", "lon", "tone")
      .select(
        col("timestamp"),
        col("gcamCode"),
        col("wordcount"),
        col("tone"),
        point(col("lon"), col("lat")).as("point")
      )
      .join(geoshape)
      .where(col("point") within col("polygon"))
      .drop("polygon", "point", "lon, lat")
      .join(gcamCodes, List("gcamCode"))
      .drop("gcamCode")
      .groupBy("timestamp", "gcam", "district")
      .agg(
        sum("wordcount").as("wordcount"),
        avg("tone").as("avgTone"),
        sum(lit(1.0)).as("count")
      )

    gdelt
      .rdd
      .flatMap(r => {
        val gc = r.getAs[String]("gcam")
        val la = r.getAs[String]("district").trim
        val ts = r.getAs[Long]("timestamp") / 1000
        val tn = r.getAs[Double]("avgTone")
        val ct = r.getAs[Double]("count")
        val wc = r.getAs[Double]("wordcount")

        List(
          s"gkg.uk.tn $ts $tn la=$la gc=$gc", // open tsdb format
          s"gkg.uk.ct $ts $ct la=$la gc=$gc",
          s"gkg.uk.wc $ts $wc la=$la gc=$gc"
        )

      })
      .coalesce(conf.partitions.apply())
      .saveAsTextFile(outputDir)

  }

}
