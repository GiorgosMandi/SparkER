package Experiments

import java.io.{ByteArrayInputStream, DataInputStream, DataOutputStream}
import java.util.Calendar

import SparkER.BlockBuildingMethods.TokenBlocking
import SparkER.BlockRefinementMethods.PruningMethods.{PruningUtils, WNP}
import SparkER.BlockRefinementMethods.{BlockFiltering, BlockPurging}
import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.{CenterClustering, EntityClusterUtils}
import SparkER.EntityMatching.EntityMatcher.compare
import SparkER.EntityMatching.{EntityMatcher, MatchingFunctions}
import SparkER.Utilities.Converters
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.WritableUtils
object EntityResolution {



  def resolution(log:Logger, separators:Array[Long], profiles:RDD[Profile], startTime: Calendar, maxProfileID:Long,
                 gt:Broadcast[Set[(Long, Long)]], newGTSize:Int, maxIdDataset1:Long, mode: String, bcstep: Int = 8,
                 partitions: Int = 0): Unit ={

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

    val numCandidates = edgesAndCount.map(_._1).sum()
    val perfectMatch = edgesAndCount.map(_._2).sum()
    val candidatePairs = edgesAndCount.flatMap(_._3)

    blocksAfterFiltering.unpersist()
    //blockIndex.unpersist()
    //profileBlocksSizeIndex.unpersist()

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
          val profilesMapRDD = profiles.map(p => (p.id, p))   // Shuffle-Write: 264mb (DBpedia)
          val commonPartitioner = new HashPartitioner(profilesPairsRDD.getNumPartitions)

          val comparisonsPerPartitionRDD = profilesPairsRDD
            .mapPartitions(partition => Iterator((TaskContext.getPartitionId(), partition.toSet))) // Shuffle-Write: 4.8Gb (DBpedia) - 241mb (_x50)
            .partitionBy(commonPartitioner)

          val profilesIDPerPartitionRDD = profilesPairsRDD
            .mapPartitions(partition => Iterator((TaskContext.getPartitionId(), partition.map(p => Set(p._1, p._2)).reduce(_++_))))

          val profilesPerPartitionRDD = profilesIDPerPartitionRDD
            .flatMap(profilesIDPerPartition => profilesIDPerPartition._2.map(pid => (pid, profilesIDPerPartition._1)))  // Shuffle-Write: 301mb  (DBpedia)
            .leftOuterJoin(profilesMapRDD)
            .map(profilesPerPartition => (profilesPerPartition._2._1, (profilesPerPartition._1, profilesPerPartition._2._2.get))) // Shuffle-Write: 6.8  (DBpedia)
            .partitionBy(commonPartitioner)
            .mapPartitions({
              p =>
                val partitionList : List[(Int, (Long, Profile))] = p.toList
                val profileMap : Map[Long, Profile] = partitionList.map(p => (p._2._1, p._2._2)).toMap
                val partitionID = partitionList.head._1
                Iterator((partitionID, profileMap))
            })

          val comparisonsAndProfilesPerPartitionRDD = comparisonsPerPartitionRDD
              .cogroup(profilesPerPartitionRDD)
              .map(t => (t._2._1.head , t._2._2.head))

          EntityMatcher.entityMatchComparisons(comparisonsAndProfilesPerPartitionRDD, threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)









        case "TD1" =>
          /** Broadcasting Comparisons in Parts */

          // k is the number of partitions and l is the size of the Triangle
          val k = profiles.getNumPartitions
          val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int]

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


          val profilesPairsRDD = candidatePairs
            .map {
              cp =>
                val baos1 = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos1), cp.firstProfileID)
                val baos2 = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos2), cp.secondProfileID)
                (baos1.toByteArray, baos2.toByteArray)
            }

          var matches : RDD[WeightedEdge] = sc.emptyRDD

          val profilesPairsSize = profilesPairsRDD.getNumPartitions
          val step = bcstep
          for  (i <- 1 to profilesPairsSize/step + 1) {
            val start = (i - 1) * step
            val end = if (start + step > profilesPairsSize) profilesPairsSize else start + step -1
            val partitions = start to end
            val profilesPairsPartRDD = profilesPairsRDD.mapPartitionsWithIndex((index, it) => if (partitions.contains(index)) it else Iterator(), true)

            val comparisons = sc.broadcast(profilesPairsPartRDD.collect())

            matches = matches union {
              distributedProfilesRDD
                .flatMap {
                  partitionProfiles =>
                    val partitionID = partitionProfiles._1
                    val profilesMap = partitionProfiles._2
                    val comp = comparisons.value
                    comp.map {
                      pair =>
                        val baos1 = new ByteArrayInputStream( pair._1)
                        val profileID1 = WritableUtils.readVLong(new DataInputStream(baos1))

                        val baos2 = new ByteArrayInputStream( pair._2)
                        val profileID2 = WritableUtils.readVLong(new DataInputStream(baos2))

                        if (profilesMap.contains(profileID1) && profilesMap.contains(profileID2)) {
                          val profile1 = profilesMap(profileID1)._1
                          val anchor1 = profilesMap(profileID1)._2

                          val profile2 = profilesMap(profileID2)._1
                          val anchor2 = profilesMap(profileID2)._2

                          if (anchor1 == anchor2) {
                            if (anchor1 == partitionID)
                              compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                            else WeightedEdge(-1, -1, -1)
                          }
                          else
                            compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                        }
                        else WeightedEdge(-1, -1, -1)
                    }
                }
                .filter(_.weight >= 0.5)
            }
          }
          matches




        case "TD2" =>
        /** Broadcasting Array of Comparisons in Parts */

          // k is the number of partitions and l is the size of the Triangle
          val k = profiles.getNumPartitions
          val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int]

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
            .cache()


          val profilesPairsRDD = candidatePairs
            .map {
              cp =>
                val baos = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos), cp.secondProfileID)
                (cp.firstProfileID, Array(baos.toByteArray))
            }
            .reduceByKey(_++_)
            .map {
              p =>
                val baos = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos), p._1)
                (baos.toByteArray, p._2)
            }
            .cache

          var matches : RDD[WeightedEdge] = sc.emptyRDD

          val profilesPairsSize = profilesPairsRDD.getNumPartitions
          val step = bcstep
          for  (i <- 1 to profilesPairsSize/step + 1) {
            val start = (i - 1) * step
            val end = if (start + step > profilesPairsSize) profilesPairsSize else start + step -1
            val partitions = start to end
            val profilesPairsPartRDD = profilesPairsRDD.mapPartitionsWithIndex((index, it) => if (partitions.contains(index)) it else Iterator(), true)

            val comparisonsIterator = sc.broadcast(profilesPairsPartRDD.collect())

            matches = matches union {
              distributedProfilesRDD
                .flatMap {
                  partitionProfiles =>
                    val partitionID = partitionProfiles._1
                    val profilesMap = partitionProfiles._2
                    val comp = comparisonsIterator.value
                    comp.flatMap {
                      pair =>
                        val baos = new ByteArrayInputStream( pair._1)
                        val profileID = WritableUtils.readVLong(new DataInputStream(baos))
                        val profileIDsArray = pair._2

                        if (profilesMap.contains(profileID)) {
                          val profile1 = profilesMap(profileID)._1
                          val anchor_1 = profilesMap(profileID)._2
                          profileIDsArray.map {
                            bytesID =>
                              val b = new ByteArrayInputStream(bytesID)
                              val id = WritableUtils.readVLong(new DataInputStream(b))

                              if (profilesMap.contains(id)) {
                                val profile2 = profilesMap(id)._1
                                val anchor_2 = profilesMap(id)._2
                                if (anchor_1 == anchor_2) {
                                  if (anchor_1 == partitionID)
                                    compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                                  else WeightedEdge(-1, -1, -1)
                                }
                                else
                                  compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                              }
                              else WeightedEdge(-1, -1, -1)
                          }
                        }
                        else Array(WeightedEdge(-1, -1, -1))
                    }
                }
                .filter(_ != null)
                .filter(_.weight >= 0.5)
            }
          }
          matches




        case "TD3" =>
          /** Broadcasting array of comparisons */

          // k is the number of partitions and l is the size of the Triangle
          val k = profiles.getNumPartitions
          val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int]

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
            .cache


          val profilesPairsRDD = candidatePairs
            .map {
              cp =>
                val baos = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos), cp.secondProfileID)
                (cp.firstProfileID, Array(baos.toByteArray))
            }
            .reduceByKey(_++_)
            .map {
              p =>
                val baos = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos), p._1)
                (baos.toByteArray, p._2)
            }
            .cache


          val comparisons = sc.broadcast(profilesPairsRDD.collect())

          distributedProfilesRDD
            .flatMap {
              partitionProfiles =>
                val partitionID = partitionProfiles._1
                val profilesMap = partitionProfiles._2
                val comp = comparisons.value
                comp.flatMap {
                  pair =>
                    val baos = new ByteArrayInputStream( pair._1)
                    val profileID = WritableUtils.readVLong(new DataInputStream(baos))
                    val profileIDsArray = pair._2

                    if (profilesMap.contains(profileID)) {
                      val profile1 = profilesMap(profileID)._1
                      val anchor_1 = profilesMap(profileID)._2
                      profileIDsArray.map {
                        bytesID =>
                          val b = new ByteArrayInputStream(bytesID)
                          val id = WritableUtils.readVLong(new DataInputStream(b))

                          if (profilesMap.contains(id)) {
                            val profile2 = profilesMap(id)._1
                            val anchor_2 = profilesMap(id)._2
                            if (anchor_1 == anchor_2) {
                              if (anchor_1 == partitionID)
                                compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                              else WeightedEdge(-1, -1, -1)
                            }
                            else
                              compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                          }
                          else WeightedEdge(-1, -1, -1)
                      }
                    }
                    else Array(WeightedEdge(-1, -1, -1))
                }
            }
            .filter(_.weight >= 0.5)





        case "TD4" =>
          /** Broadcasting comparisons */
          // k is the number of partitions and l is the size of the Triangle
          val k = profiles.getNumPartitions
          val l = ((-1 + math.sqrt(1 + 8 * k))/2).asInstanceOf[Int]

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
            .cache


          val profilesPairsRDD = candidatePairs
            .map {
              cp =>
                val baos1 = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos1), cp.firstProfileID)
                val baos2 = new ByteArrayOutputStream()
                WritableUtils.writeVLong(new DataOutputStream(baos2), cp.secondProfileID)
                (baos1.toByteArray, baos2.toByteArray)
            }
            .cache()

          val comparisons = sc.broadcast(profilesPairsRDD.collect())

          distributedProfilesRDD
            .flatMap {
              partitionProfiles =>
                val partitionID = partitionProfiles._1
                val profilesMap = partitionProfiles._2
                val comp = comparisons.value
                comp.map {
                  pair =>
                    val baos1 = new ByteArrayInputStream( pair._1)
                    val profileID1 = WritableUtils.readVLong(new DataInputStream(baos1))

                    val baos2 = new ByteArrayInputStream( pair._2)
                    val profileID2 = WritableUtils.readVLong(new DataInputStream(baos2))

                    if (profilesMap.contains(profileID1) && profilesMap.contains(profileID2)) {
                      val profile1 = profilesMap(profileID1)._1
                      val anchor1 = profilesMap(profileID1)._2

                      val profile2 = profilesMap(profileID2)._1
                      val anchor2 = profilesMap(profileID2)._2

                      if (anchor1 == anchor2) {
                        if (anchor1 == partitionID)
                          compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                        else WeightedEdge(-1, -1, -1)
                      }
                      else
                        compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                    }
                    else WeightedEdge(-1, -1, -1)
                }
            }
            .filter(_.weight >= 0.5)

        case _ =>
          //Entity matching: just a preliminary test
          val profilesMap = profiles.map(p => (p.id, p)).collectAsMap()
          val profilesBroadcast = sc.broadcast(profilesMap)
          EntityMatcher.entityMatching(profilesBroadcast = profilesBroadcast, candidatePairs = candidatePairs,
            threshold = 0.5, matchingFunction = MatchingFunctions.jaccardSimilarity)
      }

    matches.cache()
    log.info("SPARKER - Number of mathces " + matches.count())
    val endMatchTime = Calendar.getInstance()
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
