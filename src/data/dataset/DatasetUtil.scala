package data.dataset

import data.document.Document
import options.Resources.DocumentModel
import org.apache.spark.rdd.RDD

/**
  * Created by wolf on 05.04.2016.
  */
object DatasetUtil {
  def computePercentile(rdd: RDD[Int], tile: Double): Double = {
    if(rdd.count()==0) return 0.0
    // NIST method; data to be sorted in ascending order
    val r = rdd.sortBy(x => x)
    val c = r.count()
    if (c == 1) r.first()
    else {
      val n = (tile / 100d) * (c + 1d)
      val k = math.floor(n).toLong
      val d = n - k
      if (k <= 0) r.first()
      else {
        val index = r.zipWithIndex().map(_.swap)
        val last = c
        if (k >= c) {
          index.lookup(last - 1).head
        } else {
          index.lookup(k - 1).head + d * (index.lookup(k).head - index.lookup(k - 1).head)
        }
      }
    }
  }

  def panDataset(rdd:RDD[Document]) : RDD[Document]={
    //find and extract Document authors
    val rddKnown = rdd.filter(pan=>{
      pan.author!=null && pan.text != null
    })

    val rddUnknown = rdd.filter(pan=>{
      pan.author == null && pan.text != null
    })

    val rddAuthor = rdd.filter(pan=>{
      pan.author!=null && pan.text == null
    })

    val docAuthorMap = rddAuthor.map(pan=>(pan.docid,pan.author)).collectAsMap()

    val rddMapped = rddUnknown.map(pan=>{
        new Document(pan.docid, pan.doctype, docAuthorMap.getOrElse(pan.docid,null))
          .setText(pan.getText())
          .setGenre(DocumentModel.PANDOC)
    })

    val rddFiltered = rddMapped.filter(pan=>{pan.author!=null && pan.text!=null})

    rddFiltered.union(rddKnown)
  }


}
