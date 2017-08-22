package ru.consulting.bigdata.location_hours.system

/**
  * Created by VRVavilov on 03.08.2017.
  */
object HomeJob {
  val CTN = 0;
  val IMSI = 1;
  val MONTH = 2;
  val HOME_TOP_1_HOURS = 70;
  val HOME_TOP_1_BRANCH_ID = 72;
  val HOME_TOP_1_CITY_ID = 73;
  val JOB_TOP_1_HOURS = 76;
  val JOB_TOP_1_BRANCH_ID = 78;
  val JOB_TOP_1_CITY_ID = 79;

  def apply(array: Array[String]): HomeJob = new HomeJob(
    array(CTN),
    array(IMSI),
    array(MONTH),
    BigDecimal(array(HOME_TOP_1_HOURS)),
    array(HOME_TOP_1_BRANCH_ID),
    array(HOME_TOP_1_CITY_ID),
    BigDecimal(array(JOB_TOP_1_HOURS)),
    array(JOB_TOP_1_BRANCH_ID),
    array(JOB_TOP_1_CITY_ID)
  )

}
  case class HomeJob(
                             val cnt: String,
                             val imsi: String,
                             val month: String,
                             val homeTop1Hours: BigDecimal,
                             val homeTop1BranchID: String,
                             val homeTop1CityID: String,
                             val jobTop1Hours: BigDecimal,
                             val jobTop1BranchID: String,
                             val jobTop1CityID: String
                           )
