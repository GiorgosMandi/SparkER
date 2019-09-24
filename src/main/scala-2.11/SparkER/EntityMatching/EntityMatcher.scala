package SparkER.EntityMatching

import SparkER.DataStructures.{Profile, UnweightedEdge, WeightedEdge}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

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


  ///todo: tokenizzare prima di mandare in broadcast, è meglio lavorare già con (profileID -> lista token)
  def entityMatching(profilesBroadcast: Broadcast[scala.collection.Map[Long, Profile]], candidatePairs: RDD[UnweightedEdge], threshold: Double,
                     matchingFunction: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : RDD[WeightedEdge] = {
    val scored = candidatePairs.map { pair =>
      //if (profilesBroadcast.value.contains(pair.firstProfileID) && profilesBroadcast.value.contains(pair.secondProfileID)) {
      compare(profilesBroadcast.value(pair.firstProfileID), profilesBroadcast.value(pair.secondProfileID), matchingFunction)
      //}
    }

    scored.filter(_.weight >= threshold)
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
    * Iterate the RDD, perform the comparison and create an RDD of Weighted Edges
    *
    * @param comparisonsAndProfilesPerPartitionRDD An RDD that contains the IDs of the candidate Profiles in an array
    *                                              and Map with all the Profiles in the partition.
    * @param threshold the threshold which the similarity of two profiles must exceed in order to be considered as a match
    * @param matchingFunction the function which calculates the similarity of two Profiles
    * @return an RDD of weighted edges of which their profiles exceeded the threshold
    *
    */
  def entityMatchComparisons(comparisonsAndProfilesPerPartitionRDD: RDD[(Set[(Long, Long)], Map[Long, Profile])],
                 threshold: Double, matchingFunction: (Profile, Profile) => Double = MatchingFunctions.jaccardSimilarity)
  : RDD[WeightedEdge] = {
    comparisonsAndProfilesPerPartitionRDD.flatMap {
      comparisonsAndProfiles =>
        val comparisons = comparisonsAndProfiles._1
        val profilesMap = comparisonsAndProfiles._2
        comparisons.map {
          pair =>
            val profileID1 =  pair._1
            val profileID2 =  pair._2
            if (profilesMap.contains(profileID1) && profilesMap.contains(profileID2)) {
              val profile1 = profilesMap(profileID1)
              val profile2 = profilesMap(profileID2)
              compare(profile1, profile2, matchingFunction)
            }
            else {
              System.out.println("A ProfileID was missing form the Profiles Map")
              WeightedEdge(profileID1, profileID2, -1)
            }
        }
    }
    .filter(_.weight >= threshold)
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
