package SparkER.BlockBuildingMethods

import SparkER.DataStructures.KeyValue

object BlockingKeysStrategies {

  val minSuffixLen = 2
  val ngramSize = 3

  /**
    * Given a list of key-value items returns the tokens as each single word.
    *
    * @param attributes    attributes to tokenize
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    **/
  def createKeysFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ""
        }
        else {
          at.value.toLowerCase
        }
    } filter (_.trim.length > 0) flatMap {
      value =>
        value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
    }
  }

  /**
    * Given a list of key-value items returns the n-grams.
    *
    * @param attributes    attributes to transform into n-grams
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    **/
  def createNgramsFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ""
        }
        else {
          at.value.toLowerCase.trim.replace(" ", "_")
        }
    } filter (_.trim.length > 0) flatMap {
      value =>
        value.sliding(ngramSize)
    }
  }


  /**
    * Given a list of key-value items returns the suffixes.
    *
    * @param attributes    attributes to transform into n-grams
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    **/
  def createSuffixesFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = createKeysFromProfileAttributes(attributes, keysToExclude)
    tokens.flatMap(blockingKey => getSuffixes(minSuffixLen, blockingKey))
  }


  /**
    * Given a list of key-value items returns the extended suffixes.
    *
    * @param attributes    attributes to transform into n-grams
    * @param keysToExclude the item that have this keys will be excluded from the tokenize process
    **/
  def createExtendedSuffixesFromProfileAttributes(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    val tokens = createKeysFromProfileAttributes(attributes, keysToExclude)
    tokens.flatMap(blockingKey => getExtendedSuffixes(minSuffixLen, blockingKey))
  }


  /*
  * Given a blocking key, provides its suffixes
  * */
  def getSuffixes(minimumLength: Int, blockingKey: String): Set[String] = {
    var suffixes: List[String] = Nil
    if (blockingKey.length < minimumLength) {
      suffixes = blockingKey :: suffixes
    }
    else {
      val limit: Int = blockingKey.length - minimumLength + 1
      for (i <- 0 until limit) {
        suffixes = blockingKey.substring(i) :: suffixes
      }
    }
    suffixes.toSet
  }

  def getExtendedSuffixes(minimumLength: Int, blockingKey: String): Set[String] = {
    var suffixes: List[String] = List(blockingKey)
    if (minimumLength <= blockingKey.length()) {
      for (nGramSize <- blockingKey.length() - 1 to minimumLength by -1) {
        var currentPosition = 0
        val length = blockingKey.length() - (nGramSize - 1)
        while (currentPosition < length) {
          suffixes = blockingKey.substring(currentPosition, currentPosition + nGramSize) :: suffixes
          currentPosition += 1
        }
      }
    }
    suffixes.toSet
  }

  def createNgramsFromProfileAttributes2(attributes: Iterable[KeyValue], keysToExclude: Iterable[String] = Nil): Iterable[String] = {
    attributes.map {
      at =>
        if (keysToExclude.exists(_.equals(at.key))) {
          ("", "")
        }
        else {
          (at.key, at.value.toLowerCase.trim.replace(" ", "_"))
        }
    }.filter(x => x._2.trim.length > 0).flatMap { case (key, value) =>
      value.sliding(3).map(v => key + "_" + v)
    }
  }
}
