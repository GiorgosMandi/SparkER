package Experiments

import java.util.Calendar

import SparkER.BlockBuildingMethods.TokenBlocking
import SparkER.BlockRefinementMethods.PruningMethods.{PruningUtils, WNP}
import SparkER.BlockRefinementMethods.{BlockFiltering, BlockPurging}
import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.{CenterClustering, EntityClusterUtils}
import SparkER.EntityMatching.EntityMatcher.compare
import SparkER.EntityMatching.{EntityMatcher, MatchingFunctions}
import SparkER.Utilities.Converters
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


object EntityResolution {



  def resolution(log:Logger, separators:Array[Long], profiles:RDD[Profile], startTime: Calendar, maxProfileID:Long,
                 gt:Broadcast[Set[(Long, Long)]], newGTSize:Int, maxIdDataset1:Long, mode: String): Unit ={

    val sc = SparkContext.getOrCreate()

    //Token blocking
    val blocks = TokenBlocking.createBlocks(profiles, separators)
    val useEntropy = false

    blocks.cache()
    val numBlocks = blocks.count()
    val bTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks " + numBlocks)
    log.info("SPARKER - Time to blocking " + (bTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0 / 60.0 + " min")



    //Purging
    val blocksPurged = BlockPurging.blockPurging(blocks, 1.00)

    blocksPurged.cache()
    val puTime = Calendar.getInstance()
    log.info("SPARKER - Time to purging Blocks " + (puTime.getTimeInMillis - bTime.getTimeInMillis) / 1000.0 / 60.0 + " min")
    log.info("SPARKER - Number of blocks after purging " + blocksPurged.count())

    //Blocks Filtering
    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, 0.8)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separators)

    blocksAfterFiltering.cache()
    val numBlocksAfterFiltering = blocksAfterFiltering.count()
    val fTime = Calendar.getInstance()
    log.info("SPARKER - Time to filtering Blocks " + (fTime.getTimeInMillis - puTime.getTimeInMillis) / 1000.0 / 60.0 + " min")
    log.info("SPARKER - Number of blocks after filtering " +  numBlocksAfterFiltering)
    blocksPurged.unpersist()

    //Metablocking
    val blockIndexMap = blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex = sc.broadcast(blockIndexMap)  // this is 2GB on its own
    val profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = sc.broadcast(profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap())

    val blocksEntropiesMap: Broadcast[scala.collection.Map[Long, Double]] = {
      if (useEntropy) {blocksAfterFiltering
        val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
        sc.broadcast(blocksEntropies)
      }
      else {
        null
      }
    }

    blocks.unpersist()

    val edgesAndCount = WNP.WNP(
      profileBlocksFiltered,
      blockIndex.asInstanceOf[Broadcast[scala.collection.Map[Long, Array[Set[Long]]]]],
      maxProfileID.toInt,
      separators,
      gt,
      PruningUtils.ThresholdTypes.AVG,
      PruningUtils.WeightTypes.CBS,
      profileBlocksSizeIndex,
      useEntropy,
      blocksEntropiesMap,
      2.0,
      PruningUtils.ComparisonTypes.OR
    )

    //edgesAndCount.cache() //Warning: better to not cache this as in most cases it is too big and it needs to be calculated only once


    val numCandidates = edgesAndCount.map(_._1).sum()
    val perfectMatch = edgesAndCount.map(_._2).sum()
    val candidatePairs = edgesAndCount.flatMap(_._3)

    blocksAfterFiltering.unpersist()

    val pc = perfectMatch.toFloat / newGTSize.toFloat
    val pq = perfectMatch.toFloat / numCandidates.toFloat

    log.info("SPARKER - MetaBlocking PC = " + pc)
    log.info("SPARKER - MetaBlocking PQ = " + pq)
    log.info("SPARKER - Retained edges " + numCandidates)
    val endMBTime = Calendar.getInstance()

    log.info("SPARKER - Metablocking time " + (endMBTime.getTimeInMillis - fTime.getTimeInMillis) / 1000 / 60.0 + " min")

    val matches =
      mode match {
        case "JOIN" | "GL" =>
          // RDD containing the comparisons
          val profilesPairsRDD = candidatePairs.map(cp => (cp.firstProfileID, cp.secondProfileID))

          // Sets containing only the profileIDs needed to be join with the first and the second columns
          val candidateIDs1 = sc.broadcast(candidatePairs.map(cp => Set(cp.firstProfileID)).reduce(_++_))
          val candidateIDs2 = sc.broadcast(candidatePairs.map(cp => Set(cp.secondProfileID)).reduce(_++_))  // WARNING: Too many calculations

          // Filter the unnecessary Profiles. By reducing the size of the Profiles we reduce the data shuffle
          val profilesOfCandidateMapRDD1 = profiles.filter(p => candidateIDs1.value.contains(p.id)).map(p => (p.id, p))
          val profilesOfCandidateMapRDD2 = profiles.filter(p => candidateIDs2.value.contains(p.id)).map(p => (p.id, p))

          // Match the ProfileIDs with their Profiles.
          // From RDD[(profileID1, profileID2)] produce RDD[(profile, profile)]
          val candidatesRDD = profilesPairsRDD
            .leftOuterJoin(profilesOfCandidateMapRDD1)
            .map(pair => (pair._2._1,  pair._2._2.get))
            .leftOuterJoin(profilesOfCandidateMapRDD2)
            .map(pair => (pair._2._1, pair._2._2.get))

          mode match {
            case "JOIN" => EntityMatcher.entityMatchProfiles(candidatesRDD, threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)
            case "GL" => EntityMatcher.groupLinkageMatching(candidatesRDD, threshold = 0.5)
          }


        case "AGR" =>
          val profilesPairsRDD = candidatePairs.map(cp => (cp.firstProfileID, cp.secondProfileID))
          val profilesMapRDD = profiles.map(p => (p.id, p))

          // Setting the Common Partitioner in order to reduce data shuffling
          val commonPartitioner = profilesPairsRDD.partitioner match {
            case Some(p) => p
            case None => new HashPartitioner(profilesPairsRDD.partitions.length)
          }

          // Comparisons in each partition -> RDD[(PartitionID, Array[(profileID1, profileID2)])]
          // Warning: Too much time and too much memory (DBpedia, shuffle write: 4.8GB)
          //  I thought that this would not shuffle because i reduce it using its partitions IDs
          val comparisonsPerPartitionRDD = profilesPairsRDD
            .map(pair => (TaskContext.getPartitionId(), Set(pair)))
            .reduceByKey(commonPartitioner, _ ++ _)

          // The Profiles needed for the comparisons in each partition -> RDD[(PartitionID, Set[ProfileID])]
          val profilesIDPerPartitionRDD = profilesPairsRDD
            .map(pair => (TaskContext.getPartitionId(), Set(pair._1, pair._2))) //<---
            .reduceByKey(commonPartitioner, _ ++ _)

          // First flatten the set and construct -> RDD[(PartitionID, profileID)]. The join it with the profilesMapRDD
          // In the end, construct -> RDD[(PartitionID, Map[profileID -> Profile])]
          val profilesPerPartitionRDD = profilesIDPerPartitionRDD
            .flatMap(profilesIDPerPartition => profilesIDPerPartition._2.map(pid => (pid, profilesIDPerPartition._1)))
            .leftOuterJoin(profilesMapRDD)
            .map(profilesPerPartition => (profilesPerPartition._2._1, Map(profilesPerPartition._1 -> profilesPerPartition._2._2.get)))  //<--- Warning: Too much time and too much memory (DBpedia, shuffle write: 7.0GB)
            .reduceByKey(commonPartitioner, _ ++ _)

          // Join the RDD[(PartitionID, Array[(profileID1, profileID2)])] with the RDD[(PartitionID, Map[profileID -> Profile])]
          // Produce -> RDD[(Array[(profileID1, profileID2)], Map[profileID -> Profile])]
          val comparisonsAndProfilesPerPartitionRDD = comparisonsPerPartitionRDD
            .leftOuterJoin(profilesPerPartitionRDD)
            .map(tuple => (tuple._2._1, tuple._2._2.get))

          comparisonsAndProfilesPerPartitionRDD.cache

          EntityMatcher.entityMatchComparisons(comparisonsAndProfilesPerPartitionRDD, threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)

        case "AGR2" =>
          val profilesPairsRDD = candidatePairs.map(cp => (cp.firstProfileID, cp.secondProfileID))
          val profilesMapRDD = profiles.map(p => (p.id, p))   // Shuffle-Write: 264mb (DBpedia)
          val commonPartitioner = new HashPartitioner(profilesPairsRDD.getNumPartitions)

          val comparisonsPerPartitionRDD = profilesPairsRDD
            .mapPartitions(partition => Iterator((TaskContext.getPartitionId(), partition.toSet))) // Shuffle-Write: 4.8Gb (DBpedia) - 241mb (_x50)
            .partitionBy(commonPartitioner)

          val profilesIDPerPartitionRDD = profilesPairsRDD
            .mapPartitions(partition => Iterator((TaskContext.getPartitionId(), partition.map(p => Set(p._1, p._2)).reduce(_++_))))

          val profilesPerPartitionRDD = profilesIDPerPartitionRDD
            .flatMap(profilesIDPerPartition => profilesIDPerPartition._2.map(pid => (pid, profilesIDPerPartition._1)))  // Shuffle-Write: 301mb (DBpedia)
            .leftOuterJoin(profilesMapRDD)
            .map(profilesPerPartition => (profilesPerPartition._2._1, (profilesPerPartition._1, profilesPerPartition._2._2.get)))
            .partitionBy(commonPartitioner)
            .mapPartitions({
            p =>
              val partitionID = p.next()._1
              var profileMap : Map[Long, Profile] = Map()
              p.foreach(partitionProfile => profileMap += (partitionProfile._2._1 -> partitionProfile._2._2))
              Iterator((partitionID, profileMap))
          })

          /*val profilesPerPartitionRDD = profilesIDPerPartitionRDD
            .flatMap(profilesIDPerPartition => profilesIDPerPartition._2.map(pid => (pid, profilesIDPerPartition._1)))  // Shuffle-Write: 301mb (DBpedia)
            .leftOuterJoin(profilesMapRDD)
            .map(profilesPerPartition => (profilesPerPartition._2._1, Map(profilesPerPartition._1 -> profilesPerPartition._2._2.get)))  //Shuffle-Write: 7.0GB DBpedia -  409mb (_x50)
            .reduceByKey(commonPartitioner, _ ++ _)*/

          val comparisonsAndProfilesPerPartitionRDD = comparisonsPerPartitionRDD
              .cogroup(profilesPerPartitionRDD)
              .map(t => (t._2._1.head , t._2._2.head))

          /*val comparisonsAndProfilesPerPartitionRDD = comparisonsPerPartitionRDD      //Shuffle-Read: 650mb (_x50)
            .leftOuterJoin(profilesPerPartitionRDD)
            .map(tuple => (tuple._2._1, tuple._2._2.get))*/

          //comparisonsAndProfilesPerPartitionRDD.cache

          EntityMatcher.entityMatchComparisons(comparisonsAndProfilesPerPartitionRDD, threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)

        case "TD" =>
          val profilesPairs = sc.broadcast(candidatePairs.map(cp => (cp.firstProfileID, cp.secondProfileID)).collect())

          val k = profiles.getNumPartitions
          val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int]

          def getPartitionID(p:Int, q:Int) : Int = {
            val id = (2 * l - p + 2) * (p - 1)/2 + (q - p + 1)
            id.asInstanceOf[Int]
          }

          val distributedProfilesRDD = profiles
            .map {
              p =>
                var partitionIDs : Set[Int] = Set[Int]()
                val rand = scala.util.Random
                rand.setSeed(p.id)
                val anchor = rand.nextInt(l) + 1
                for (p <- 1 to anchor) partitionIDs += getPartitionID(p, anchor)
                for (q <- anchor to l) partitionIDs += getPartitionID(anchor, q)
                (partitionIDs, p, anchor)
            }
            .flatMap {
              profileAndPartitionIDs =>
                val profileMap = Map(profileAndPartitionIDs._2.asInstanceOf[Profile].id -> (profileAndPartitionIDs._2, profileAndPartitionIDs._3))
                profileAndPartitionIDs._1.map(partitionID => (partitionID, profileMap))
            }
            .reduceByKey(_++_)

         distributedProfilesRDD
            .flatMap {
              partitionProfiles =>
                val partitionID = partitionProfiles._1
                val profilesMap = partitionProfiles._2
                profilesPairs.value.map {
                  pair =>
                    val profileID1 = pair._1
                    val profileID2 = pair._2

                    if (profilesMap.contains(profileID1) && profilesMap.contains(profileID2)) {
                      val profile1 = profilesMap(profileID1)._1
                      val profile2 = profilesMap(profileID2)._1
                      val anchor_1 = profilesMap(profileID1)._2
                      val anchor_2 = profilesMap(profileID2)._2
                      if (anchor_1 == anchor_2) {
                        if (anchor_1 == partitionID) compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                        else null
                      }
                      else
                        compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                    }
                    else null
                }
            }
           .filter(_ != null)
           .filter(_.weight >= 0.5)


        case _ =>
          //Entity matching: just a preliminary test
          val profilesPairs = candidatePairs.map(cp => (cp.firstProfileID, cp.secondProfileID))
          val profilesMap = profiles.map(p => (p.id, p)).collectAsMap()
          val profilesBroadcast = sc.broadcast(profilesMap)
          EntityMatcher.entityMatching(profilesBroadcast = profilesBroadcast, candidatePairs = candidatePairs,
            threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)
      }

    matches.cache()
    val endMatchTime = Calendar.getInstance()
    log.info("SPARKER - Number of mathces " + matches.count())
    log.info("SPARKER - Matching time " + (endMatchTime.getTimeInMillis - endMBTime.getTimeInMillis) / 1000 / 60.0 + " min")

    // unpersisting all the persisted RDDs
    val rdds = sc.getPersistentRDDs
    rdds.keySet.foreach (key => rdds(key).unpersist())

    //Clustering
    val clusters = CenterClustering.getClusters(profiles = profiles, edges = matches, maxProfileID = maxProfileID.toInt,
      edgesThreshold = 0.5, separatorID = maxIdDataset1)
    clusters.cache()
    val numClusters = clusters.count()

    matches.unpersist()


    val endClusterTime = Calendar.getInstance()
    log.info("SPARKER - Clustering time " + (endClusterTime.getTimeInMillis - endMatchTime.getTimeInMillis) / 1000 / 60.0 + " min")

    val clusterPcPq = EntityClusterUtils.calcPcPqCluster(clusters, gt, maxIdDataset1)

    log.info("SPARKER - Generated clusters: "+numClusters)
    log.info("SPARKER - Clustering PC " + clusterPcPq._1)
    log.info("SPARKER - Clustering PQ " + clusterPcPq._2)


    val endTime = Calendar.getInstance()
    log.info("SPARKER - Execution time " + (endTime.getTimeInMillis - startTime.getTimeInMillis) / 1000 / 60.0 + " min")

  }


  }
