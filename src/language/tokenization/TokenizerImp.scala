package language.tokenization

/**
  * Created by wolf on 26.03.2016.
  */
trait TokenizerImp extends Serializable{
  def tokenize(sentence:String):Seq[String]

}
