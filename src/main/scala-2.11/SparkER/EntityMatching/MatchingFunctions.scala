package SparkER.EntityMatching

import SparkER.DataStructures.{KeyValue, Profile}
import scala.collection.mutable

object MatchingFunctions {

  def getWords(p: Profile, n:Int = 1): Set[String] = {
    p.attributes
      .map(_.value)
      .flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING))
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .sliding(n)
      .map(_.mkString(" "))
      .toSet
  }

  def getWordsFromKeyValues(attribute: KeyValue, n:Int = 1): Set[String] = {
    attribute.value
      .split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .sliding(n)
      .map(_.mkString(" "))
      .toSet
  }

  def getCharacters(p: Profile, n:Int = 1): Set[String] = {
    p.attributes
      .map(_.value)
      .flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING))
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .flatMap(_.toCharArray)
      .sliding(n)
      .map(_.mkString(""))
      .toSet
  }


  def getCharactersFrequency(p: Profile, n:Int = 1): Map[String, Int] = {
    p.attributes
      .map(_.value)
      .flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING))
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .flatMap(_.toCharArray)
      .sliding(n)
      .toList
      .map(_.mkString(""))
      .groupBy(x => x)
      .map(x => (x._1, x._2.length))
  }


  def getWordsFrequency(p: Profile, n:Int = 1 ): Map[String, Int] = {
    p.attributes
      .map(_.value)
      .flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING))
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .sliding(n)
      .toList
      .map(_.mkString(" "))
      .groupBy(x => x)
      .map(x => (x._1, x._2.length))
  }


  def getVectorMagnitude(vector: Map[String, Int], totalSize: Double): Double ={
    val maginitude = vector.map(t =>  Math.pow(t._2.toDouble / totalSize, 2.0)).sum
    Math.sqrt(maginitude)
  }


  def jaccardSimilarity(getTokens: (Profile, Int) => Set[String], ngramsSize:Int = 1)(p1: Profile, p2: Profile): Double = {
    val t1 = getTokens(p1, ngramsSize)
    val t2 = getTokens(p2, ngramsSize)
    val common = t1.intersect(t2).size.toDouble
    common / (t1.size + t2.size - common)
  }

  def jaccardSimilarity(k1: KeyValue, k2: KeyValue): Double = {
    val t1 = getWordsFromKeyValues(k1)
    val t2 = getWordsFromKeyValues(k2)
    val common = t1.intersect(t2).size.toDouble
    common / (t1.size + t2.size - common)
  }


  def enhancedJaccardSimilarity(getTokensFrequency: (Profile, Int) => Map[String, Int], ngramsSize:Int = 1)(p1: Profile, p2: Profile): Double = {

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getTokensFrequency(p1, ngramsSize)
    val itemVector2 = getTokensFrequency(p2, ngramsSize)

    //calculate the total tokens of the entities
    val totalTerms1 = itemVector1.keySet.size
    val totalTerms2 = itemVector2.keySet.size

    val vector1IsSmaller = totalTerms1 < totalTerms2
    val maxItemVector = { if (vector1IsSmaller) itemVector2 else itemVector1}
    val minItemVector = { if (vector1IsSmaller) itemVector1 else itemVector2}

    val numerator = maxItemVector
      .filter(t => minItemVector.contains(t._1))
      .map(t => Math.min(t._2, minItemVector(t._1)))
      .sum
      .toDouble

    val denominator = totalTerms1 + totalTerms2 - numerator.toDouble;

    numerator / denominator
  }


  def generalizedJaccardSimilarity(getTokensFrequency: (Profile, Int) => Map[String, Int], ngramsSize:Int = 1)(p1: Profile, p2: Profile): Double ={

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getTokensFrequency(p1, ngramsSize)
    val itemVector2 = getTokensFrequency(p2, ngramsSize)

    //calculate the total tokens of the entities
    val totalTerms1 = itemVector1.size
    val totalTerms2 = itemVector2.size

    val vector1IsSmaller = totalTerms1 < totalTerms2
    val maxItemVector = { if (vector1IsSmaller) itemVector2 else itemVector1}
    val minItemVector = { if (vector1IsSmaller) itemVector1 else itemVector2}
    val maxTotalTerms = { if (vector1IsSmaller) totalTerms2 else totalTerms1}
    val minTotalTerms = { if (vector1IsSmaller) totalTerms1 else totalTerms2}

    val numerator = maxItemVector
      .map(t => Math.min(t._2.toDouble/maxTotalTerms, minItemVector.getOrElse(t._1, 0).toDouble/minTotalTerms))
      .sum

    var allKeys = maxItemVector.keySet
    allKeys ++= minItemVector.keySet
    val denominator = allKeys
      .map(key => Math.max(minItemVector.getOrElse(key, 0).toDouble/maxTotalTerms, minItemVector.getOrElse(key, 0).toDouble/minTotalTerms))
      .sum

    numerator / denominator
  }


  def cosineSimilarity(getTokensFrequency: (Profile, Int) => Map[String, Int], ngramsSize:Int = 1)(p1: Profile, p2: Profile ): Double = {

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getTokensFrequency(p1, ngramsSize)
    val itemVector2 = getTokensFrequency(p2, ngramsSize)

    //calculate the total tokens of the entities
    val totalTerms1 = itemVector1.keySet.size
    val totalTerms2 = itemVector2.keySet.size

    val vector1IsSmaller = totalTerms1 < totalTerms2
    val maxItemVector = { if (vector1IsSmaller) itemVector2 else itemVector1}
    val minItemVector = { if (vector1IsSmaller) itemVector1 else itemVector2}
    // calculate the TF Cosine similarity
    val numerator = maxItemVector
      .filter(t => minItemVector.contains(t._1))
      .map(t => (t._2 * minItemVector(t._1)).toDouble / totalTerms1 / totalTerms2)
      .sum
    val denominator = getVectorMagnitude(itemVector1, totalTerms1) * getVectorMagnitude(itemVector2, totalTerms2)
    numerator / denominator
  }


  def getSimilarityEdges(profile1: Profile, profile2: Profile, threshold: Double = 0.5)
  : mutable.PriorityQueue[(Double, (String, String))] = {

    var similarityQueue = mutable.PriorityQueue[(Double, (String, String))]()
    for (attrIndex1 <- profile1.attributes.zipWithIndex;
         attrIndex2 <- profile2.attributes.zipWithIndex)  {

      val attr1 = attrIndex1._1
      val index1 = attrIndex1._2
      val attr2 = attrIndex2._1
      val index2 = attrIndex2._2

      val sim = jaccardSimilarity(attr1, attr2)
      if (sim > threshold) {
        val edge = ( sim.toDouble, ("a" + index1, "b" + index2))
        similarityQueue += edge
      }
    }
    similarityQueue
  }

}
