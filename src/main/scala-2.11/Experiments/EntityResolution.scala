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
import org.apache.hadoop.io.file.tfile.Utils
import org.apache.hadoop.io.VLongWritable
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.Try
object EntityResolution {



  def resolution(log:Logger, separators:Array[Long], profiles:RDD[Profile], startTime: Calendar, maxProfileID:Long,
                 gt:Broadcast[Set[(Long, Long)]], newGTSize:Int, maxIdDataset1:Long, mode: String, bcstep: Int = 8,
                 partitions: Int = 0): Unit ={

    val sc = SparkContext.getOrCreate()

    //Token blocking
    val blocks = TokenBlocking.createBlocks(profiles, separators)
    val useEntropy = false

    blocks.setName("Blocks").cache()
    val numBlocks = blocks.count()
    val bTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks " + numBlocks)
    log.info("SPARKER - Time to blocking " + (bTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0 / 60.0 + " min")



    //Purging
    val blocksPurged = BlockPurging.blockPurging(blocks, 1.00)

    blocksPurged.setName("BloksPurged").cache()
    val puTime = Calendar.getInstance()
    log.info("SPARKER - Time to purging Blocks " + (puTime.getTimeInMillis - bTime.getTimeInMillis) / 1000.0 / 60.0 + " min")
    log.info("SPARKER - Number of blocks after purging " + blocksPurged.count())

    //Blocks Filtering
    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, 0.8)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separators)

    blocksAfterFiltering.setName("BlocksAfterFiltering").cache()
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
    blockIndex.unpersist()
    profileBlocksSizeIndex.unpersist()

    val pc = perfectMatch.toFloat / newGTSize.toFloat
    val pq = perfectMatch.toFloat / numCandidates.toFloat

    log.info("SPARKER - MetaBlocking PC = " + pc)
    log.info("SPARKER - MetaBlocking PQ = " + pq)
    log.info("SPARKER - Retained edges " + numCandidates)
    val endMBTime = Calendar.getInstance()

    log.info("SPARKER - Metablocking time " + (endMBTime.getTimeInMillis - fTime.getTimeInMillis) / 1000 / 60.0 + " min")




    /** Entity Matching
      * Distribute Profiles using the Triangle Distribution method
      * Broadcast the Array of Comparisons in Parts
      * */

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
          (partitionIDs, Array((p, anchor)))
      }
      .flatMap(p => p._1.map(id => (id, p._2)))
      .reduceByKey(_++_)
      .map {
        profileAndPartition =>
          val partitionID = profileAndPartition._1
          val profiles = profileAndPartition._2
          val profileAr = profiles.map(p =>  Array((p._1, p._2))).reduce(_++_)
          (partitionID, profileAr)
      }
      .setName("DistributedProfiles")
      .persist(StorageLevel.MEMORY_AND_DISK)




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
        comparisonMap.keySet.map(key => key +: comparisonMap(key)).toIterator
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
      val ComparisonsPerPartitionPart = ComparisonsPerPartitionRDD
        .mapPartitionsWithIndex((index, it) => if (partitions.contains(index)) it else Iterator(), true)
        .collect()

      val comparisonsIterator = sc.broadcast(ComparisonsPerPartitionPart)

      val wEdges =
        distributedProfilesRDD
          .flatMap {
            partitionProfiles =>
              val partitionID = partitionProfiles._1
              val profilesArray = partitionProfiles._2
              val comparisonsArray = comparisonsIterator.value
              val edges = comparisonsArray
                .filter(ar => profilesArray.map(_._1.id.toInt).contains(ar.head))
                .flatMap {
                  comparisons =>
                    val profileID = comparisons.head
                    val profileNode = profilesArray.find(p => p._1.id.toInt == profileID).head
                    val profile1 = profileNode._1
                    val anchor1 = profileNode._2
                    comparisons
                      .tail
                      .filter(profilesArray.map(_._1.id.toInt).contains)
                      .filter {
                        id =>
                          var pass = true
                          val anchor2 = profilesArray.find(p => p._1.id.toInt == id).head._2
                          if (anchor1 == anchor2 && anchor1 != partitionID) pass = false
                          pass
                      }
                      .map {
                        id =>
                          val profile2 = profilesArray.find(p => p._1.id.toInt == id).head._1
                          compare(profile1, profile2, MatchingFunctions.jaccardSimilarity)
                      }
                }
              comparisonsIterator.unpersist()
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

    log.info("SPARKER - Number of mathces " + matchesCount)
    val endMatchTime = Calendar.getInstance()
    log.info("SPARKER - Matching time " + (endMatchTime.getTimeInMillis - endMBTime.getTimeInMillis) / 1000 / 60.0 + " min")

    // unpersisting all the persisted RDDs
    val rdds = sc.getPersistentRDDs
    rdds.filter(rdd => rdd._2.name != "Matches").foreach(_._2.unpersist())


    //Clustering
    val clusters = CenterClustering.getClusters(profiles = profiles, edges = matches, maxProfileID = maxProfileID.toInt,
      edgesThreshold = 0.5, separatorID = maxIdDataset1)
    clusters.setName("Clusters").cache()
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
