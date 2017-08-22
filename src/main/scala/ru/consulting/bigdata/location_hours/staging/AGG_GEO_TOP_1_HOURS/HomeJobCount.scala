package ru.consulting.bigdata.location_hours.staging.AGG_GEO_TOP_1_HOURS

import org.apache.spark.rdd.RDD
import ru.consulting.bigdata.location_hours.system.{HomeJob, Parametrs}

/**
  * Created by VRVavilov on 03.08.2017.
  */

object HomeJobCount {
  def loadHomeJob(arrayRDD: RDD[Array[String]]): RDD[HomeJob] ={
    arrayRDD.map(x=>(HomeJob(x)))
  }
  def count(arrayRDD: RDD[Array[String]]): Unit ={
    var rddHomeJob=loadHomeJob(arrayRDD)
    var rddtmp = rddHomeJob.filter(x =>(x.imsi.substring(0,5).equals(Parametrs.CHECK_INSI)))
      .map(x=>(x.cnt, x.month, roundDigit(x.homeTop1Hours),x.homeTop1BranchID,x.homeTop1CityID,
        roundDigit(x.jobTop1Hours),x.jobTop1BranchID,x.jobTop1CityID))
    val agg1 = rddtmp.map(x => ((x._2,x._3,x._4,x._5),1)).reduceByKey((x,y) => (x+y))
    val agg2 = rddtmp.map(x => ((x._2,x._6,x._7,x._8),1)).reduceByKey((x,y) => (x+y))
    val join = agg1.fullOuterJoin(agg2)
      .map(x=> ((x._1._1,x._1._3,x._1._4,x._1._2),(x._2._1.getOrElse(0),x._2._2.getOrElse(0))))
      .sortBy(_._1, ascending=true).map(x=> (x._1._1,x._1._2,x._1._3,x._1._4,x._2._1,x._2._2))

    join.coalesce(1,true).saveAsTextFile(Parametrs.PATH_OUTPUT)
    //join.collect().foreach(println(_))
  }
  def roundDigit(x:BigDecimal): BigDecimal={
    if(x<5)
      5
    else {
      var y:BigDecimal = x/5
      y.setScale(0, scala.math.BigDecimal.RoundingMode.HALF_DOWN)*5
    }
  }
}
