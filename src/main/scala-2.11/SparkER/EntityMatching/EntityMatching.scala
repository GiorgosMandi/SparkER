package SparkER.EntityMatching

import SparkER.DataStructures.{Profile, UnweightedEdge, WeightedEdge}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable


object EntityMatching {


  /**
    * Aggregate the comparisons of each Profile. The produced RDD will look like RDD[(Profile, Array(ProfileID)]
    *
    * @param candidatePairs   RDD containing the IDs of the profiles that must be compared
    * @param profiles         RDD of the Profiles
    * @return                 RDD[(profile, Array(profileID))] for which the profile must be compared with all
    *                         the profiles that their ids are in the Array(profileID)
    */
  def getComparisons(candidatePairs : RDD[UnweightedEdge], profiles : RDD[Profile]) : RDD[(Profile, Array[Int])] ={
    candidatePairs
      .mapPartitions { p =>
        var comparisonMap: Map[Int, Array[Int]] = Map()
        p.foreach {
          cp =>
            val id1 = cp.firstProfileID.toInt
            val id2 = cp.secondProfileID.toInt
            if (comparisonMap.contains(id1)) {
              val comp: Array[Int] = comparisonMap(id1) :+ id2
              comparisonMap += (id1 -> comp)
            }
            else {
              val comp: Array[Int] = Array(id2)
              comparisonMap += (id1 -> comp)
            }
        }
        comparisonMap.keySet.map(key => (key, comparisonMap(key))).toIterator
      }
      .reduceByKey(_++_)
      .leftOuterJoin(profiles.map(p => (p.id.toInt, p)))
      .map(p => (p._2._2.get, p._2._1))
  }

  /**
    * For each partition return a Map with all the Profiles of the partition
    * The produced RDD will look like RDD[Map(ID -> Profile)]
    *
    * @param profiles   RDD containing the Profiles
    * @return           RDD in which each partition contains Map(ID -> Profile)
    */
  def getProfilesMap(profiles : RDD[Profile] ) : RDD[Map[Int, Profile]] = {
    profiles
      .mapPartitions {
        partitions =>
          val m = partitions.map(p => Map(p.id.toInt -> p)).reduce(_++_)
          Iterator(m)
      }
  }

  /**
    * Compare the input profiles and calculate the Weighted Edge that connects them
    *
    * @param profile1           Profile to be compared
    * @param profile2           Profile to be compared
    * @param matchingFunction   function that compares the profiles - returns weight
    * @return                   a WeightedEdge that connects these profiles
    */
  def profileMatching(profile1: Profile, profile2: Profile, matchingFunction: (Profile, Profile) => Double)
  :WeightedEdge = {
    val similarity = matchingFunction(profile1, profile2)
    WeightedEdge(profile1.id, profile2.id, similarity)
  }



  /**
    * Compare all the attributes of the Profiles and construct a Queue of similarity edges.
    * Then use it in order to calculate the similarity between the input Profiles
    *
    * @param profile1      Profile to be compared
    * @param profile2      Profile to be compared
    * @param threshold     the threshold which the similarity of two profiles must exceed in order
    *                      to be considered as a match
    * @return              a Weighted Edge which connects the input profiles
    */
  def groupLinkage(profile1: Profile, profile2: Profile, threshold: Double = 0.5)
  : WeightedEdge = {

    val similarityQueue: mutable.PriorityQueue[(Double, (String, String))] = MatchingFunctions.getSimilarityEdges(profile1, profile2, threshold)

    if (similarityQueue.nonEmpty ) {
      var edgesSet: Set[String] = Set()
      var nominator = 0.0
      var denominator = (profile1.attributes.length + profile2.attributes.length).toDouble
      similarityQueue.dequeueAll.foreach {
        edge =>
          if (!edgesSet.contains(edge._2._1) && !edgesSet.contains(edge._2._2)) {
            edgesSet += (edge._2._1, edge._2._2)
            nominator += edge._1
            denominator -= 1.0
          }
      }
      WeightedEdge(profile1.id, profile2.id, nominator / denominator)
    }
    else
      WeightedEdge(profile1.id, profile2.id, -1)
  }


  /** Entity Matching
    * Distribute Profiles using the Triangle Distribution method
    * Broadcast the Array of Comparisons in Parts
    *
    *  @param profiles            RDD containing the profiles.
    *  @param candidatePairs      RDD containing UnweightedEdges
    *  @param bcstep              the number of partitions that will be broadcasted in each iteration
    *  @return                    an RDD of WeightedEdges and its size
    * */
  def entityMatching(profiles : RDD[Profile], candidatePairs : RDD[UnweightedEdge], bcstep : Int,
                     matchingMethod : (Profile, Profile, (Profile, Profile) => Double ) => WeightedEdge,
                     matchingFunctions: (Profile, Profile) => Double)
  : (RDD[WeightedEdge], Long) = {

    val sc = SparkContext.getOrCreate()

    val comparisonsRDD = getComparisons(candidatePairs, profiles)
      .setName("ComparisonsPerPartition")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val profilesMap = getProfilesMap(profiles)
      .setName("profilesMap")
      .cache()

    // In a loop collect and broadcast partitions of the RDD and perform the comparisons.
    var matches : RDD[WeightedEdge] = sc.emptyRDD
    var matchesCount : Long = 0

    val ComparisonsPerPartitionSize = comparisonsRDD.getNumPartitions
    val step = if (bcstep > 0) bcstep else ComparisonsPerPartitionSize
    val partitionGroupIter = (0 until comparisonsRDD.getNumPartitions).grouped(step)

    while (partitionGroupIter.hasNext){
      val partitionGroup = partitionGroupIter.next()

      // collect and broadcast the comparisons
      val comparisonsArrayBD = sc.broadcast(
        comparisonsRDD
          .mapPartitionsWithIndex((index, it) => if (partitionGroup.contains(index)) it else Iterator(), preservesPartitioning = true)
          .collect()
      )
      // perform comparisons
      val wEdges = profilesMap
        .mapPartitions {
          partitionProfiles =>
            val profilesM = partitionProfiles.next()
            comparisonsArrayBD
              .value
              .flatMap {
                c =>
                  val profile1 = c._1
                  val comparisons = c._2
                  comparisons
                    .filter(profilesM.contains)
                    .map(profilesM(_))
                    .map(p => matchingMethod(profile1, p, matchingFunctions))
                    .filter(_.weight >= 0.5)
              }
              .toIterator
        }

      matches = matches union wEdges

      //cache only if it's the final execution
      if (!partitionGroupIter.hasNext)
        matches.setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)

      // if count occurs outside of the for loop, then the Executors will collect all the parts of the comparisonsRDD
      matchesCount += wEdges.count()
      comparisonsArrayBD.unpersist()
    }

    (matches, matchesCount)
  }

  def entityMatchingAlt(profiles : RDD[Profile], candidatePairs : RDD[UnweightedEdge], bcstep : Int,
                     matchingMethod : (Profile, Profile, (Profile, Profile) => Double ) => WeightedEdge,
                     matchingFunctions: (Profile, Profile) => Double)
  : (RDD[WeightedEdge], Long) = {

    val comparisonsRDD = getComparisons(candidatePairs, profiles)
      .flatMap(x => x._2.map(id => (id, Array(x._1)))) // Warning: Shuffle-Write is too much!!
      .reduceByKey(_++_)
      .setName("ComparisonsPerPartition")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val profilesID = profiles.map(p => (p.id.toInt, p))

    val profilesComparisons = profilesID.leftOuterJoin(comparisonsRDD)
    val matches = profilesComparisons
      .filter(comparisons => !comparisons._2._2.getOrElse(Array()).isEmpty)
      .flatMap {
        comparisons =>
          val profile1 = comparisons._2._1
          val profilesArray = comparisons._2._2.get
          profilesArray
            .map(profile2 => matchingMethod(profile1, profile2, matchingFunctions))
            .filter(_.weight >= 0.5)
      }
      .setName("Matches")
      .persist(StorageLevel.MEMORY_AND_DISK)

    (matches, matches.count())

    }

}
