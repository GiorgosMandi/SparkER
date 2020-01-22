package Experiments

import java.util.Calendar

import SparkER.BlockBuildingMethods.TokenBlocking
import SparkER.BlockRefinementMethods.PruningMethods.{CEP, CNP, PruningUtils, WEP, WNP}
import SparkER.BlockRefinementMethods.{BlockFiltering, BlockPurging}
import SparkER.DataStructures.Profile
import SparkER.EntityClustering.{CenterClustering, EntityClusterUtils, UniqueMappingClustering}
import SparkER.EntityMatching.{EntityMatching, MatchingFunctions}
import SparkER.Utilities.Converters
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object EntityResolution {

  def resolution(log:Logger, separators:Array[Long], profiles:RDD[Profile], profiles_count: Long,
                 maxProfileID:Long, gt:Broadcast[Set[(Long, Long)]], newGTSize:Int, maxIdDataset1:Long,
                 metablocking: String, bcstep: Int = 8, partitions: Int = 0): Unit ={

    val sc = SparkContext.getOrCreate()

    //Token blocking
    val startTime =  Calendar.getInstance()
    val blocks = TokenBlocking.createBlocks(profiles, separators)
    val useEntropy = false
    blocks.setName("Blocks").cache()
    val numBlocks = blocks.count()
    val bTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks " + numBlocks)
    log.info("SPARKER - Time to blocking " + (bTime.getTimeInMillis - startTime.getTimeInMillis) / 1000.0 / 60.0 + " min")


    //Purging
    val blocksPurged = BlockPurging.blockPurging(blocks, 1.025)

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

    val blockIndex = sc.broadcast(blockIndexMap) // < ----- WARNING this broadcasted variable is too big

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
          profiles_count,
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

    edgesAndCount.setName("edgesAndCount").persist(StorageLevel.MEMORY_AND_DISK) // WARNING: this might be dangerous for big datasets

    val numCandidates = edgesAndCount.map(_._1).sum()
    val perfectMatch = edgesAndCount.map(_._2).sum()
    val candidatePairs = edgesAndCount.flatMap(_._3)
    blockIndex.unpersist()
    profileBlocksSizeIndex.unpersist()
    blocksAfterFiltering.unpersist()
    profileBlocks.unpersist()

    val pc = perfectMatch.toFloat / newGTSize.toFloat
    val pq = perfectMatch.toFloat / numCandidates.toFloat

    log.info("SPARKER - MetaBlocking PC = " + pc)
    log.info("SPARKER - MetaBlocking PQ = " + pq)
    log.info("SPARKER - Retained edges " + numCandidates)
    val endMBTime = Calendar.getInstance()

    log.info("SPARKER - Metablocking time " + (endMBTime.getTimeInMillis - fTime.getTimeInMillis) / 1000 / 60.0 + " min")

    // initialize the getTokens functions (either words or char and n-grams size) and the similarity function
    val getTokens = (p:Profile, i:Int) => MatchingFunctions.getCharactersFrequency(p, i)
    val mf = MatchingFunctions.cosineSimilarity(getTokens, 2)(_, _)
    val (matches , matchesCount) = EntityMatching.entityMatchingJOIN(profiles, candidatePairs, bcstep,
      //matchingFunctions = MatchingFunctions.chfCosineSimilarity)
      matchingFunctions = mf)

    val endMatchTime = Calendar.getInstance()
    log.info("SPARKER - Number of mathces " + matchesCount)
    log.info("SPARKER - Matching time " + (endMatchTime.getTimeInMillis - endMBTime.getTimeInMillis) / 1000 / 60.0 + " min")

    edgesAndCount.unpersist()
    blockIndex.destroy()
    profileBlocksSizeIndex.destroy()

    //Clustering
    val clusters = CenterClustering.getClusters(profiles = profiles, edges = matches, maxProfileID = maxProfileID.toInt,
      edgesThreshold = 0.5, separatorID = maxIdDataset1)
    //val clusters = UniqueMappingClustering.getClusters(profiles, edges = matches, maxProfileID = maxProfileID.toInt, edgesThreshold = 0.5, separatorID = maxIdDataset1)

    clusters.setName("Clusters").cache()
    val numClusters = clusters.count()

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
