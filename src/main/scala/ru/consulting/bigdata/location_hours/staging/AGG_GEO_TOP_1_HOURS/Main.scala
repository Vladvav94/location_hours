package ru.consulting.bigdata.location_hours.staging.AGG_GEO_TOP_1_HOURS

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import ru.consulting.bigdata.location_hours.system.Parametrs

/**
  * Created by VRVavilov on 02.08.2017.
  */
object Main {
  def main(args:Array[String]):Unit={
   // val fs = FileSystem.get(new Configuration())
   // val params = Parameters.instance(args, fs)

    val sparkConf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory","1g")
    val sc = new SparkContext(sparkConf)
    val arrayRDD = Read.sequenceFileValue(sc, Parametrs.PATH_INPUT,";", classOf[NullWritable], classOf[Text])
    HomeJobCount.count(arrayRDD)
  }

}