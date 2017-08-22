package ru.consulting.bigdata.location_hours.staging.AGG_GEO_TOP_1_HOURS

import org.apache.commons.math3.stat.inference.TestUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by VRVavilov on 02.08.2017.
  */
object Read {
  def sequenceFileValue(sc: SparkContext, pathToFile: String, separator: String, key: Class[_],value:Class[_]):RDD[Array[String]]={
    sc.sequenceFile(pathToFile, key, value)
      .map(x => x._2.toString.split(separator,-1))
  }
}


