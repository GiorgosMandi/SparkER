package SparkER.EntityMatching

import SparkER.DataStructures.{KeyValue, Profile}

import scala.collection.mutable

object MatchingFunctions {

  def getTokens(p: Profile): Set[String] = {
    p.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).filter(_.length > 0).toSet
  }

  def getTokens(attribute: KeyValue): Set[String] = {
    attribute.value.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map(_.toLowerCase.trim).filter(_.length > 0).toSet
  }

  def getCharacters(p: Profile, n:Int): Set[String] = {
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

  def getCharacters(attribute: KeyValue, n:Int): Set[String] = {
    attribute.value
      .split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
      .map(_.toLowerCase.trim)
      .filter(_.length > 0)
      .flatMap(_.toCharArray)
      .sliding(n)
      .map(_.mkString(""))
      .toSet
  }


  def getNGramsFrequency(p: Profile, n: Int = 1 ): List[(String, Int)] = {
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
      .toList
  }


  def getVectorMagnitude(vector: List[(String, Int)], totalSize: Double): Double ={
    val maginitude = vector.map(t => Math.pow(t._2.toDouble / totalSize, 2.0)).sum
    Math.sqrt(maginitude)
  }


  def jaccardSimilarity(p1: Profile, p2: Profile): Double = {
    val t1 = getTokens(p1)
    val t2 = getTokens(p2)
    val common = t1.intersect(t2).size.toDouble
    common / (t1.size + t2.size - common)
  }

  def jaccardSimilarity(k1: KeyValue, k2: KeyValue): Double = {
    val t1 = getTokens(k1)
    val t2 = getTokens(k2)
    //val t1 = getCharacters(k1, 2)
    //val t2 = getCharacters(k2, 2)
    val common = t1.intersect(t2).size.toDouble
    common / (t1.size + t2.size - common)
  }


  def enhancedJaccardSimilarity(p1: Profile, p2: Profile): Double = {

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getNGramsFrequency(p1)
    val itemVector2 = getNGramsFrequency(p2)

    //calculate the total tokens of the entities
    val totalTerms1 = p1.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble
    val totalTerms2 = p2.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble

    val vector1IsSmaller = itemVector1.size < itemVector2.size
    val maxItemVector = { if (vector1IsSmaller) itemVector2 else itemVector1}
    val minItemVector = { if (vector1IsSmaller) itemVector1 else itemVector2}

    val mapMinVector = minItemVector.toMap
    val numerator = maxItemVector
      .filter(t => mapMinVector.contains(t._1))
      .map(t => Math.min(t._2, mapMinVector(t._1)))
      .sum

    val denominator = totalTerms1 + totalTerms2 - numerator;

    numerator / denominator
  }



  // TODO: Something is wrong -- Too many matches
  def tfGeneralizedJaccardSimilarity(p1: Profile, p2: Profile): Double ={

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getNGramsFrequency(p1)
    val itemVector2 = getNGramsFrequency(p2)

    //calculate the total tokens of the entities
    val totalTerms1 = p1.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble
    val totalTerms2 = p2.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble


    val vector1IsSmaller = itemVector1.size < itemVector2.size
    val maxItemVector = { if (vector1IsSmaller) itemVector2 else itemVector1}
    val minItemVector = { if (vector1IsSmaller) itemVector1 else itemVector2}
    val maxTotalTerms = { if (vector1IsSmaller) totalTerms2 else totalTerms1}
    val minTotalTerms = { if (vector1IsSmaller) totalTerms1 else totalTerms2}

    val mapMinVector = minItemVector.toMap
    val mapMaxVector = maxItemVector.toMap


    val numerator = maxItemVector
      .map(t => Math.min(t._2.toDouble/maxTotalTerms, {if(mapMinVector.contains(t._1)) mapMinVector(t._1).toDouble else 0.0} /minTotalTerms))
      .sum

    var allKeys = mapMaxVector.keySet
    allKeys ++= mapMinVector.keySet
    val denominator = allKeys
      .map(key => Math.max({
        if (mapMaxVector.contains(key)) mapMaxVector(key).toDouble else 0.0
      } / maxTotalTerms, {
        if (mapMinVector.contains(key)) mapMinVector(key).toDouble else 0.0
      } / minTotalTerms))
      .sum

    numerator / denominator
  }


  def tfCosineSimilarity(p1: Profile, p2: Profile ): Double = {

    // calculate the frequencies of the tokens/ngrams
    val itemVector1 = getNGramsFrequency(p1)
    val itemVector2 = getNGramsFrequency(p2)

    //calculate the total tokens of the entities
    val totalTerms1 = p1.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble
    val totalTerms2 = p2.attributes.map(_.value).flatMap(_.split(SparkER.BlockBuildingMethods.BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).map(_.toLowerCase.trim).count(_.length > 0).toDouble

    val maxItemVector = itemVector1
    val minItemVector = itemVector2
    if (itemVector1.size < itemVector2.size){
      val maxItemVector = itemVector2
      val minItemVector = itemVector1
    }

    // calculate the TF Cosine similarity
    val mapMinVector = minItemVector.toMap
    val numerator = maxItemVector
      .filter(t => mapMinVector.contains(t._1))
      .map(t => (t._2 * mapMinVector(t._1)).toDouble / totalTerms1 / totalTerms2)
      .sum

    val denominator = getVectorMagnitude(itemVector1, totalTerms1) * getVectorMagnitude(itemVector2, totalTerms1)

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
