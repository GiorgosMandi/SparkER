package Experiments

import java.util.Calendar

import SparkER.BlockBuildingMethods.TokenBlocking
import SparkER.BlockRefinementMethods.PruningMethods.{CEP, CNP, PruningUtils, WEP, WNP}
import SparkER.BlockRefinementMethods.{BlockFiltering, BlockPurging}
import SparkER.DataStructures.{Profile, WeightedEdge}
import SparkER.EntityClustering.{CenterClustering, EntityClusterUtils}
import SparkER.EntityMatching.EntityMatcher.compare
import SparkER.EntityMatching.{EntityMatcher, MatchingFunctions}
import SparkER.Utilities.Converters
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object EntityResolution {



  def resolution(log:Logger, separators:Array[Long], profiles:RDD[Profile], startTime: Calendar, maxProfileID:Long,
                 gt:Broadcast[Set[(Long, Long)]], newGTSize:Int, maxIdDataset1:Long, mode: String, metablocking: String,
                 bcstep: Int = 8, partitions: Int = 0): Unit ={

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
      if (useEntropy) {
        val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
        sc.broadcast(blocksEntropies)
      }
      else {
        null
      }
    }

    blocks.unpersist()


    val edgesAndCount = metablocking match {
      case "CEP" =>
        CEP.CEP(
          profileBlocksFiltered,
          blockIndex.asInstanceOf[Broadcast[scala.collection.Map[Long, Array[Set[Long]]]]],
          maxProfileID.toInt,
          separators,
          gt,
          PruningUtils.WeightTypes.CBS,
          profileBlocksSizeIndex,
          useEntropy,
          blocksEntropiesMap
        )
      case "WEP" =>
        WEP.WEP(
          profileBlocksFiltered,
          blockIndex.asInstanceOf[Broadcast[scala.collection.Map[Long, Array[Set[Long]]]]],
          maxProfileID.toInt,
          separators,
          gt,
          PruningUtils.WeightTypes.CBS,
          profileBlocksSizeIndex,
          useEntropy,
          blocksEntropiesMap
        )
      case "CNP" =>
        CNP.CNP(
          blocks,
          profiles.count(),
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
          PruningUtils.ComparisonTypes.OR
        )
      case _ =>
        WNP.WNP(
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
    }

    /*
    val candidatePairs = edgesAndCount.flatMap(_._3)
    log.info("SPARKER ")
    log.info("SPARKER ")
    log.info("SPARKER ")
    log.info("SPARKER - Profiles = " + profiles.count() + " Comparisons = " + candidatePairs.count())
    log.info("SPARKER ")
    log.info("SPARKER ")
    log.info("SPARKER ")*/


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

    val (matches , matchesCount) = EntityMatcher.entityMatching(profiles, candidatePairs, bcstep)

    val endMatchTime = Calendar.getInstance()
    log.info("SPARKER - Number of mathces " + matchesCount)
    log.info("SPARKER - Matching time " + (endMatchTime.getTimeInMillis - endMBTime.getTimeInMillis) / 1000 / 60.0 + " min")

    // unpersisting all the persisted RDDs
    val rdds = sc.getPersistentRDDs
    rdds.filter(rdd => rdd._2.name != "Matches").foreach(_._2.unpersist())


    //Clustering
    val clusters = CenterClustering.getClusters(profiles = profiles, edges = matches, maxProfileID = maxProfileID.toInt,
      edgesThreshold = 0.5, separatorID = maxIdDataset1)
    clusters.setName("Clusters").cache()
    val numClusters = clusters.count()

    matches.unpersist() // <-


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
