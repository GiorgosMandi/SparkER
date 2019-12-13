package Experiments

import java.util.Calendar

import SparkER.DataStructures.Profile
import SparkER.Wrappers.{CSVWrapper, JSONWrapper}
import org.apache.log4j.{LogManager, SimpleLayout}
import org.apache.spark.{ SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD


/**
  * Experiments
  *
  * @author Luca Gagliardelli
  * @since 18/12/2018
  **/
object Main {



  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)

    val startTime = Calendar.getInstance()

    val conf = new SparkConf()
      .setAppName("SparkER")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // Parsing the input arguments
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
        list match {
          case Nil => map
          case "-d1" :: value :: tail =>
            nextOption(map ++ Map("d1" -> value), tail)
          case "-d2" :: value :: tail =>
            nextOption(map ++ Map("d2" -> value), tail)
          case "-gt" :: value :: tail =>
            nextOption(map ++ Map("gt" -> value), tail)
          case "-log" :: value :: tail =>
            nextOption(map ++ Map("log" -> value), tail)
          case "-mode" :: value :: tail =>
            nextOption(map ++ Map("mode" -> value), tail)
          case "-sep" :: value :: tail =>
            nextOption(map ++ Map("sep" -> value), tail)
          case "-partitions" :: value :: tail =>
            nextOption(map ++ Map("partitions" -> value), tail)
          case "-step" :: value :: tail =>
            nextOption(map ++ Map("bcstep" -> value), tail)
          case "-metablocking" :: value :: tail =>
            nextOption(map ++ Map("metablocking" -> value), tail)
          case option :: tail => println("Unknown option " + option)
            nextOption(map ++ Map("unknown" -> ""), tail)
        }
    }

    val arglist = args.toList
    type OptionMap = Map[String, String]
    val options = nextOption(Map(), arglist)

    if(!options.contains("d1") || !options.contains("gt")){
      log.error("SPARKER - Necessary arguments are missing!")
      System.exit(1)
    }
    // Reading and setting log file
    val logFilePath =
      if (options.contains("log")) options("log")
      else {
        System.out.println("ERROR: No input file")
        System.exit(1)
        null
    }

    val layout = new SimpleLayout()
    /*val appender = new FileAppender(layout, logFilePath, false)
    log.addAppender(appender)*/

    val metablocking =
      if (options.contains("metablocking"))
        options("metablocking")
      else ""

    def isAllDigits(x: String) = x forall Character.isDigit
    val bcstep : Int =
      if (options.contains("bcstep") &&  isAllDigits(options("bcstep") ))
        options("bcstep").toInt
    else 0
    if (bcstep > 0 ) log.info("SPARKER - Broadcast Step " + bcstep)

    // choose between clean-clean or dirty ER
    if (options.contains("d2")) {
      val (profiles, newGT, separators) = cleanCleanER(options, log)
      val maxProfileID = profiles.map(_.id).max()
      val maxIdDataset1 = profiles.filter(_.sourceId == 1).map(_.id).max()
      val newGTSize = newGT.size
      val gt = sc.broadcast(newGT)
      EntityResolution.resolution(log, separators, profiles, startTime, maxProfileID, gt, newGTSize, maxIdDataset1,  metablocking, bcstep)

    } else {
      val (profiles, newGT) = dirtyER(options, log)
      val newGTSize = newGT.size
      val gt = sc.broadcast(newGT)
      val maxProfileID = profiles.map(_.id).max()
      EntityResolution.resolution(log, Array(), profiles, startTime, maxProfileID, gt, newGTSize, -1,  metablocking, bcstep)
    }

    //System.in.read()
    sc.stop()
  }


  /**
    * Parse the input arguments and calculate the ncessary variables for
    * Clean-Clean ER execution.
    *
    * @param options input command line arguments
    * @param log logger
    * @return the loaded profiles as RDD, the loaded ground truth and an array of separators
    */
  def cleanCleanER(options : Map[String, String], log : Logger) : (RDD[Profile], Set[(Long, Long)], Array[Long]) ={
    val sc = SparkContext.getOrCreate()

    val separator =
      if (options.contains("sep")) options("sep")
      else ","

    // Reading datasets
    // Dataset 1
    val startTime = Calendar.getInstance()
    val dataset1Path = options("d1")
    val datasetExtension1 = dataset1Path.toString.split("\\.").last
    val dataset1 =
     datasetExtension1 match {
        case "csv" => CSVWrapper.loadProfiles2(dataset1Path, realIDField = "id", separator = separator, sourceId = 1, header = true)
        case "json" => JSONWrapper.loadProfiles(dataset1Path, realIDField = "realProfileID", sourceId = 1)
        case _ => {
          log.error("SPARKER - This filetype is not supported yet")
          System.exit(1)
          null
        }
      }

    val maxIdDataset1 = dataset1.map(_.id).max()

    // Dataset 2
    val dataset2Path = options("d2")
    val datasetExtension2 = dataset2Path.toString.split("\\.").last
    val dataset2 =
      datasetExtension2 match {
        case "csv" => CSVWrapper.loadProfiles2(dataset2Path, realIDField = "id", separator = separator, sourceId = 2, header = true, startIDFrom = maxIdDataset1 + 1)
        case "json" => JSONWrapper.loadProfiles(dataset2Path, realIDField = "realProfileID", sourceId = 2, startIDFrom = maxIdDataset1 + 1)
        case _ => {
          log.error("SPARKER - This filetype is not supported yet")
          System.exit(1)
          null
        }
      }

    // Reading input profiles
    val separators = Array(maxIdDataset1)
    var profiles = dataset1.union(dataset2)
    if (options.contains("partitions"))  profiles = profiles.repartition(options("partitions").toInt)
    profiles.setName("Profiles").cache()

    val pTime = Calendar.getInstance()
    log.info("SPARKER - Loaded profiles " + profiles.count())
    log.info("SPARKER - Profiles Partitions " + profiles.getNumPartitions)
    log.info("SPARKER - Time to load profiles " + (pTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0 / 60.0 + " min")

    // Reading Ground-Truth dataset
    val groundtruthPath = options("gt")

    val gtExtension2 = groundtruthPath.toString.split("\\.").last
    val groundtruth = gtExtension2 match {
      case "csv" => CSVWrapper.loadGroundtruth(groundtruthPath, separator, header = true)
      case "json" => JSONWrapper.loadGroundtruth(groundtruthPath, firstDatasetAttribute = "id1", secondDatasetAttribute = "id2")
      case _ => {
        log.error("SPARKER - This filetype is not supported yet")
        System.exit(1)
        null
      }
    }

    //Converts the ids in the groundtruth to the autogenerated ones
    val realIdIds1 = sc.broadcast(dataset1.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())

    val realIdIds2 = sc.broadcast(dataset2.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())

    var newGT: Set[(Long, Long)] = null
    newGT = groundtruth.map { g =>
      val first = realIdIds1.value.get(g.firstEntityID)
      val second = realIdIds2.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        val f = first.get
        val s = second.get
        if (f < s) {
          (f, s)
        }
        else {
          (s, f)
        }
      }
      else {
        (-1L, -1L)
      }
    }.filter(_._1 >= 0).collect().toSet

    val gtTime = Calendar.getInstance()
    log.info("SPARKER - Time to load groundtruth " + (gtTime.getTimeInMillis - pTime.getTimeInMillis) / 1000.0 / 60.0 + " min")

    (profiles, newGT, separators)

  }



  /**
    * Parse the input arguments and calculate the ncessary variables for
    * Dirty ER execution.
    *
    * @param options input command line arguments
    * @param log logger
    * @return the loaded profiles as RDD and the loaded ground truth
    */
  def dirtyER(options : Map[String, String], log : Logger) : (RDD[Profile], Set[(Long, Long)]) ={
    val sc = SparkContext.getOrCreate()

    val separator =
      if (options.contains("sep")) options("sep")
      else ","

    // Reading datasets
    // Dataset 1
    val startTime = Calendar.getInstance()
    val dataset1Path = options("d1")
    val datasetExtension1 = dataset1Path.toString.split("\\.").last
    val dataset1 =
      datasetExtension1 match {
        case "csv" => CSVWrapper.loadProfiles2(dataset1Path, separator = separator, header = true)
        case "json" => JSONWrapper.loadProfiles(dataset1Path)
        case _ => {
          log.error("SPARKER - This filetype is not supported yet")
          System.exit(1)
          null
        }
      }

    var profiles = dataset1
    if (options.contains("partitions"))  profiles = profiles.repartition(options("partitions").toInt)
    profiles.setName("Profiles").cache()

    val pTime = Calendar.getInstance()
    log.info("SPARKER - Loaded profiles " + profiles.count())
    log.info("SPARKER - Profiles Partitions " + profiles.getNumPartitions)
    log.info("SPARKER - Time to load profiles " + (pTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0 / 60.0 + " min")

    // Reading Ground-Truth dataset
    val groundtruthPath = options("gt")
    val gtExtension2 = groundtruthPath.toString.split("\\.").last
    val groundtruth = gtExtension2 match {
      case "csv" => CSVWrapper.loadGroundtruth(groundtruthPath, separator, header = true)
      case "json" => JSONWrapper.loadGroundtruth(groundtruthPath, firstDatasetAttribute = "id1", secondDatasetAttribute = "id2")
      case _ => {
        log.error("SPARKER - This filetype is not supported yet")
        System.exit(1)
        null
      }
    }

    var newGT: Set[(Long, Long)] = null
    newGT = groundtruth.map { g =>
      val f = g.firstEntityID.toLong
      val s = g.secondEntityID.toLong
      if (f < s)
        (f, s)
      else
        (s, f)
    }.filter(_._1 >= 0).collect().toSet

    val gtTime = Calendar.getInstance()
    log.info("SPARKER - Time to load groundtruth " + (gtTime.getTimeInMillis - pTime.getTimeInMillis) / 1000.0 / 60.0 + " min")

    (profiles, newGT)

  }
}

