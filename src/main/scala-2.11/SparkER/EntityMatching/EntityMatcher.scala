package SparkER.EntityMatching

import java.util.Calendar

import SparkER.DataStructures.{Profile, UnweightedEdge, WeightedEdge}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable



object EntityMatcher {

  /**
    * Compare the input profiles and calculate the Weighted Edge that connects them
    *
    * @param profile1
    * @param profile2
    * @param matchingFunction keys to exclude from the blocking process
    * @return a WeightedEdge that connects these profiles
    */
  def compare(profile1: Profile, profile2: Profile, matchingFunction: (Profile, Profile) => Double)
  :WeightedEdge = {
    val similarity = matchingFunction(profile1, profile2)
    WeightedEdge(profile1.id, profile2.id, similarity)
  }


  /** Entity Matching
    * Distribute Profiles using the Triangle Distribution method
    * Broadcast the Array of Comparisons in Parts
    *
    *  @param profiles RDD containing the profiles.
    *  @param candidatePairs RDD containing UnweightedEdges
    *  @param bcstep the number of partitions that will be broadcasted in each iteration
    *  @return an RDD of WeightedEdges and its size
    * */
  def entityMatching(profiles : RDD[Profile], candidatePairs : RDD[UnweightedEdge], bcstep : Int)
  : (RDD[WeightedEdge], Long) = {

      val sc = SparkContext.getOrCreate()

      // k is the number of partitions and l is the size of the Triangle
      val k = profiles.getNumPartitions
      val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int] + 1

      // calculate the ID of the partition
      // p and q are the coordinates of the position in the triangle
      def getPartitionID(p:Int, q:Int) : Int = {
        val id = (2 * l - p + 2) * (p - 1)/2 + (q - p + 1)
        id.asInstanceOf[Int]
      }


      // distribute the profiles in the triangle
      // for each profile get a random anchor (between [1, l]) and send it to
      // all the executor with p = anchor and q = anchor

      val distributedProfilesRDD = profiles
        .map {
          p =>
            var partitionIDs : Set[Int] = Set[Int]()
            val rand = scala.util.Random
            //rand.setSeed(p.id)
            val anchor = rand.nextInt(l) + 1
            val partitionIDs1 = (1 to anchor).map(p => getPartitionID(p, anchor))
            val partitionIDs2 = (anchor to l).map(q => getPartitionID(anchor, q))
            partitionIDs ++= partitionIDs1 ++ partitionIDs2
            (partitionIDs, (p, anchor))
        }
        .flatMap(p => p._1.map(id => (id, Array((p._2._1.id.toInt, p._2)))))
        .reduceByKey(_ ++ _)
       // .map (p => (p._1, p._2, p._2.zipWithIndex.map(pt => (pt._1._1.id.toInt, pt._1)).toMap))
        .setName("DistributedProfiles")
        .persist(StorageLevel.MEMORY_AND_DISK)


      // for info in DistributedProfiles
      // val t = distributedProfilesRDD.collect()
      // val partitionSizes = t.map(p => (p._1,p._2.length)).sorted.map(_._2)
      // val As =t.flatMap(p=> p._2.map(pa => (pa._2._2, pa._2._1.id))).groupBy(_._1).map(p => (p._1, p._2.distinct.length)).toList.sorted



      // Transform the candidatePairs into an RDD in which each partition contain arrays of comparisons.
      // In these arrays the first element is the ProfileID that must be compared with all the other
      // IDs in the array.
      val ComparisonsPerPartitionRDD = candidatePairs
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
        .setName("ComparisonsPerPartition")
        .persist(StorageLevel.MEMORY_AND_DISK)

      // In a for loop, collect and broadcast some of the partitions of the RDD and perform the comparisons.
      var matches : RDD[WeightedEdge] = sc.emptyRDD
      var matchesCount : Long = 0

      val ComparisonsPerPartitionSize = ComparisonsPerPartitionRDD.getNumPartitions
      val step = bcstep
      for (i <- 1 to ComparisonsPerPartitionSize/step + 1) {
        val start = (i - 1) * step
        val end = if (start + step > ComparisonsPerPartitionSize) ComparisonsPerPartitionSize else start + step -1
        val partitions = start to end

        val BDcomparisonsIterator = sc.broadcast(
          ComparisonsPerPartitionRDD
            .mapPartitionsWithIndex((index, it) => if (partitions.contains(index)) it else Iterator(), true)
            .collect()
        )

        val wEdges =
          distributedProfilesRDD
            .flatMap {
              partitionProfiles =>
                val partitionID = partitionProfiles._1
                val profilesAr = partitionProfiles._2
                //val profilePosMap = partitionProfiles._3
                val profileSetIDs = profilesAr.map(_._1).toSet // WARNING: significant memory allocation
                val comparisonsArray = BDcomparisonsIterator.value
                val edges = comparisonsArray
                  .filter(t => profileSetIDs.contains(t._1)) // WARNING: Performance cost
                  .flatMap {
                    comparisons =>
                      val profileNode =  profilesAr.find(_._1 == comparisons._1).get._2 // WARNING: Performance cost - maybe i sould use maps
                      val profile1 = profileNode._1
                      val anchor1 = profileNode._2
                      val intersection = profileSetIDs.intersect(comparisons._2.toSet) // WARNING: Performance cost
                      val profiles = profilesAr.filter(t => intersection.contains(t._1))
                      comparisons._2
                        .filter(intersection.contains)
                        .map(id => profiles.find(_._1 == id).get._2)
                        .filter( (anchor1 == _._2 && anchor1 != partitionID))
                        .map {
                          pn =>
                            val profile2 = pn._1
                            compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                        }
                  }
                BDcomparisonsIterator.unpersist()
                edges
            }
            .filter(_.weight >= 0.5)

        matches = matches union wEdges
        if ( i == ComparisonsPerPartitionSize/step + 1)
          matches.setName("Matches").persist(StorageLevel.MEMORY_AND_DISK)  //if it's the final execution cache it

        // if the count occur outside of the for loop, then the Executors will collect all the parts of the
        // ComparisonsPerPartitionRDD and they will probably run out of memory
        matchesCount += wEdges.count()
      }

    (matches, matchesCount)

  }


  /**
    * Iterate the RDD, perform the comparison and create an RDD of Weighted Edges
    *
    * @param candidatesRDD RDD containing pair of Profiles to be compared
    * @param threshold the threshold which the similarity of two profiles must exceed in order
    *                  to be considered as a match
    * @param matchingFunction the function which calculates the similarity of two Profiles
    * @return an RDD of weighted edges of which their profiles exceeded the threshold
    */
  def entityMatchProfiles(candidatesRDD: RDD[(Profile, Profile)], threshold: Double,
                  matchingFunction: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : RDD[WeightedEdge] = {
    val scored = candidatesRDD.map { pair =>
      compare(pair._1, pair._2, matchingFunction)
    }
    scored.filter(_.weight >= threshold)
  }





  /**
    * Iterate RDD and perform Group Linkage Entity Mathiching
    * @param candidatesRDD RDD containing pair of Profiles to be compared
    * @param threshold the threshold which the similarity of two profiles must exceed in order to be considered as a match
    * @return an RDD of weighted edges of which their profiles exceeded the threshold
    */
  def groupLinkageMatching(candidatesRDD: RDD[(Profile, Profile)], threshold: Double) : RDD[WeightedEdge] = {
    candidatesRDD.map(p => groupLinkage(p._1, p._2, threshold)).filter(_.weight >= 0)
  }


  /**
    * Compare all the attributes of the Profiles and construct a Queue of similarity edges.
    * Then use it in order to calculate the similarity between the input Profiles
    *
    * @param profile1
    * @param profile2
    * @param threshold the threshold which the similarity of two profiles must exceed in order to be considered as a match
    * @return a Weighted Edge which connects the input profiles
    */
  def groupLinkage(profile1: Profile, profile2: Profile, threshold: Double)
  : WeightedEdge ={

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

}
