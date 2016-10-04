package language.postagger

/**
  * Created by wolf on 24.04.2016.
  */
trait PosTaggerImp extends Serializable{
  def testTokens(sentence:Seq[String]):Seq[(String,String)]
  def test(sentence:Seq[String]):Seq[String]

}
