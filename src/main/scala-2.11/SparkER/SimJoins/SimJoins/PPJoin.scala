package SparkER.SimJoins.SimJoins

import SparkER.DataStructures.Profile
import SparkER.SimJoins.DataStructure.DocIndex
import SparkER.SimJoins.Commons.CommonFunctions
import SparkER.SimJoins.Commons.JS.{CommonJsFunctions, JsFilters}
import org.apache.spark.rdd.RDD

object PPJoin {


  /**
    * Ritorna le coppie candidate che passano il length filter usando l'indice
    **/
  def getPairs(prefixIndex: RDD[(Long, Array[(Long, Int, Int)])], threshold: Double): RDD[((Long, Long), (DocIndex, DocIndex))] = {
    prefixIndex.flatMap { case (tokenId, docs) =>
      var results: List[((Long, Long), (DocIndex, DocIndex))] = Nil
      var i = 0
      while (i < docs.length - 1) {
        var j = i + 1
        while ((j < docs.length) && (docs(i)._3 >= docs(j)._3 * threshold)) {
          if (docs(i)._1 < docs(j)._1) {
            results = ((docs(i)._1, docs(j)._1), (DocIndex(docs(i)._2, docs(i)._3), DocIndex(docs(j)._2, docs(j)._3))) :: results
          }
          else {
            results = ((docs(j)._1, docs(i)._1), (DocIndex(docs(j)._2, docs(j)._3), DocIndex(docs(i)._2, docs(i)._3))) :: results
          }
          j = j + 1
        }
        i += 1
      }
      results
    }
  }

  /**
    * Unisce le coppie candidate ottenendo i dati che servono per il position filter
    **/
  def mergePairs(pairs: RDD[((Long, Long), (DocIndex, DocIndex))]): RDD[((Long, Long), ((DocIndex, DocIndex), Int))] = {
    def createCombiner(e: (DocIndex, DocIndex)): ((DocIndex, DocIndex), Int) = (e, 1)

    def mergeValue(c: ((DocIndex, DocIndex), Int), e: (DocIndex, DocIndex)): ((DocIndex, DocIndex), Int) = {
      ((DocIndex(Math.max(c._1._1.pos, e._1.pos), c._1._1.docLen), DocIndex(Math.max(c._1._2.pos, e._2.pos), c._1._2.docLen)), c._2 + 1)
    }

    def mergeCombiners(c1: ((DocIndex, DocIndex), Int), c2: ((DocIndex, DocIndex), Int)): ((DocIndex, DocIndex), Int) = {
      ((DocIndex(Math.max(c1._1._1.pos, c2._1._1.pos), c1._1._1.docLen), DocIndex(Math.max(c1._1._2.pos, c2._1._2.pos), c1._1._2.docLen)), c1._2 + c2._2)
    }

    pairs.combineByKey(createCombiner, mergeValue, mergeCombiners)
  }

  /**
    * Applica il position filter alle coppie che hanno superato il length filter
    **/
  def applyPositionFilter(pairs: RDD[((Long, Long), ((DocIndex, DocIndex), Int))], threshold: Double): RDD[(Long, Long)] = {
    pairs.filter { pair =>
      val common = pair._2._2
      val (d1: DocIndex, d2: DocIndex) = pair._2._1
      JsFilters.positionFilter(d1.docLen, d2.docLen, d1.pos, d2.pos, common, threshold)
    }.map(x => x._1)
  }

  /** Ritorna l'elenco di coppie candidate */
  def getCandidates(documents: RDD[(Long, String)], threshold: Double): (RDD[(Long, Array[Long])], RDD[(Long, Long)]) = {
    val tokenizedDocSort = CommonJsFunctions.tokenizeAndSort(documents)
    val prefixIndex = CommonJsFunctions.buildPrefixIndex(tokenizedDocSort, threshold)
    val pairsLen = getPairs(prefixIndex, threshold)
    val pairs = mergePairs(pairsLen)
    val candidates = applyPositionFilter(pairs, threshold)
    (tokenizedDocSort, candidates)
  }

  /** PPJoin con condizioni multiple in Spark */
  def getMatchesMulti(profiles: RDD[Profile], attributesThresholds: Map[String, Double]): RDD[(Long, Long)] = {
    val tokenizedAndCandidates = attributesThresholds.map { case (attribute, threshold) =>
      val docs = CommonFunctions.extractField(profiles, attribute)
      (attribute, getCandidates(docs, threshold))
    }

    val tokenizedAttributes = tokenizedAndCandidates.map(x => (x._1, x._2._1))
    val candidates = tokenizedAndCandidates.map(_._2._2).reduce((c1, c2) => c1.intersection(c2))

    val docs = tokenizedAndCandidates.toList.map { case (attribute, (tokenizedDocs, _)) =>
      tokenizedDocs.map(d => (d._1, (attribute, d._2)))
    }.reduce((x, y) => x.union(y)).groupByKey().map(x => (x._1, x._2.toMap))

    val docBroadcast = profiles.context.broadcast(docs.collectAsMap())

    val pairs = candidates.filter { case (doc1, doc2) =>
      val d1 = docBroadcast.value.get(doc1)
      val d2 = docBroadcast.value.get(doc2)
      var pass = true
      if (d1.isDefined && d2.isDefined) {
        val docs1 = d1.get
        val docs2 = d2.get
        val it = attributesThresholds.iterator
        while (it.hasNext && pass) {
          val (attribute, threshold) = it.next()
          pass = CommonJsFunctions.passJS(docs1.getOrElse(attribute, Array.empty[Long]), docs2.getOrElse(attribute, Array.empty[Long]), threshold)
        }
      }
      else {
        pass = false
      }
      pass
    }

    pairs

    /*val pairs = docs.join(candidates).map(x => (x._2._2, (x._1, x._2._1))).join(docs).map(x => ((x._1, x._2._2), x._2._1))

    pairs.filter { case (doc1, doc2) =>
      val it = attributesThresholds.iterator
      var pass = false
      while (it.hasNext && pass) {
        val (attribute, threshold) = it.next()
        pass = CommonJsFunctions.passJS(doc1._2.getOrElse(attribute, Array.empty[Long]),
          doc2._2.getOrElse(attribute, Array.empty[Long]), threshold)
      }
      pass
    }.map { p =>
      if (p._1._1 < p._2._1) {
        (p._1._1, p._2._1)
      }
      else {
        (p._2._1, p._1._1)
      }
    }*/
  }
}
