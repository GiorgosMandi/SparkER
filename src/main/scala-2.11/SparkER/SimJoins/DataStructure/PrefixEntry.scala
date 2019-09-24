package SparkER.SimJoins.DataStructure

case class PrefixEntry(docId: Long, tokenPos: Int, docLen: Int) extends Ordered[PrefixEntry]{

  override def compare(that: PrefixEntry): Int = {
    this.docLen.compare(that.docLen)
  }
}
