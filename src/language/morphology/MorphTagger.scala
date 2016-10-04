package language.morphology

import language.postagger.PosTaggerCRF
import options.ParamsMorphology
import options.Resources.MorphologyResources

import scala.util.control.Breaks

/**
  * Created by wolf on 25.04.2016.
  */
class MorphTagger extends Serializable {

  val labels = Array[LabelSet](MorphTagSet, AgreementTagSet, CaseTagSet, VerbTagSet, PosTagSet, DerivationTagSet)
  val models = Array[String](MorphologyResources.crfMorphoTag, MorphologyResources.crfAggrementTag,
    MorphologyResources.crfCaseTag, MorphologyResources.crfVerbTag, MorphologyResources.crfPosTag,
    MorphologyResources.crfDerivationTag)

  val taggers = models.zipWithIndex.map { case (filename, index) => new PosTaggerCRF(labels(index), filename) }
  val analyzer = ParamsMorphology.analyzer
  val tokenizer = ParamsMorphology.tokenizer
  val distance = new Distance()

  def predict(tokens: Seq[String], tagStrings: Seq[MorphLight]): Seq[String] = {

    val tagTokens = taggers.map(posTagger => {
      posTagger.test(tokens)
    })

    var tagVote: Seq[Seq[String]] = Seq.fill(tokens.length)(Seq[String]())

    for (toki <- 0 until tokens.length) {
      for (resi <- 0 until tagTokens.length) {
        val update = tagTokens(resi)(toki)
        if (!update.equals("NONE")) {
          tagVote = tagVote.updated(toki, tagVote(toki) :+ update)
        }
      }
    }

    tagVote.zipWithIndex.map { case (morph, index) => {
      val result = tagStrings(index).results
      val indice = distance.weighted(result, morph)._2
      result.apply(indice)
    }
    }
  }

  def predictTags(tokens: Seq[String]): Seq[Seq[String]] = {

    val tagTokens = taggers.par.map(posTagger => {
      posTagger.test(tokens)
    }).toArray

    var tagVote: Seq[Seq[String]] = Seq.fill(tokens.length)(Seq[String]())

    for (toki <- 0 until tokens.length) {
      for (resi <- 0 until tagTokens.length) {
        if (!tagTokens(resi)(toki).equals("NONE")) {
          tagVote = tagVote.updated(toki, tagVote(toki) :+ tagTokens(resi)(toki))
        }
      }
    }

    tagVote.map(seq => {
      seq.distinct
    })
  }

  def predictFlatTags(tokens:Seq[String]):Seq[String]={
    predictTags(tokens).flatten
  }

  def predict(tokens: Seq[String]): Seq[String] = {
    val tagStrings = tokens.map(token => analyzer.analyzeAsLight(token))
    predict(tokens, tagStrings)
  }

  def train(trainFilename: String): this.type = {
    val data = MorphologyResources.loadMorphology(trainFilename)
    taggers.par.foreach(tagger => {
      tagger.train(data)
    })
    this
  }

  /**
    *
    * @param trainFilename
    * @param testFilename
    * @return token and sentence accuracy
    */
  def evaluate(trainFilename: String, testFilename: String): (Double, Double, Seq[(String, String, String)]) = {
    val data = MorphologyResources.loadMorphology(testFilename)
    var sentp = 0d
    var toktp = 0d
    var tokct = 0d
    val senct = data.length
    println("Training is started ...")
    train(trainFilename)
    println("Training is finished.")
    println("Evaluation is started ...")

    var falsePositives = Seq[(String, String, String)]()

    data.foreach { case (sindex, sentence, morphs) => {
      val tokens = morphs.map(morph => morph.token)
      val predictions = predict(tokens, morphs)
      var correct = true
      predictions.zipWithIndex.foreach { case (prediction, index) => {
        val actual = morphs(index).label
        val token = morphs(index).token
        if (prediction.equals(actual)) {
          toktp += 1
        }
        else {
          correct = false
          falsePositives = falsePositives :+ (token, actual, prediction)

        }
      }
      }
      if (correct) sentp += 1
      tokct += tokens.length
    }
    }

    (toktp / tokct, sentp / senct, falsePositives)

  }

}

object MorphTagger extends MorphTagger {
  def main(args: Array[String]): Unit = {
    val devFilename = MorphologyResources.sentenceMorphTrainDev
    val testFilename = MorphologyResources.sentenceMorphTest

    //println(predict(Seq("Onun","masalı","evi","var",".")))
    val tuple = evaluate(devFilename, devFilename)
    println(tuple._1, tuple._2)
    tuple._3.foreach(println)
  }
}

class Distance() extends Serializable {


  def jaccard(tagStrings: Seq[String], tags: Seq[String]): (Double, Int) = {
    val vectors = tagStrings.map(tagStr => tagStr.split("\\+"))
    //Similarity
    val scores = vectors.map(vector => {
      val intersect = vector.intersect(tags)
      val union = vector.union(tags)
      val total = tags.length + vector.length
      if (total == 0) 1.0
      else intersect.length.toDouble / (total - intersect.length)
    })

    val pair = scores.zipWithIndex.maxBy(_._1)
    pair
  }

  def weighted(tagStrings: Seq[String], tags: Seq[String]): (Double, Int) = {
    val vectors = tagStrings.map(tagStr => tagStr.split("\\+"))
    val weights = tags.distinct
      .map(tag => {
        (tag, tags.count(tg => tag.equals(tg)))
      })
      .toMap
    val scores = vectors.map(vector => {
      val intersection = vector.intersect(weights.keySet.toSeq)
      val intersectionScore = vector.map(tag => {
        weights.getOrElse(tag, 0)
      })
        .sum
        .toDouble
      val totalLength = vector.length + tags.length
      intersectionScore / (totalLength - intersection.length)
    })

    scores.zipWithIndex.maxBy(_._1)

  }


}


class LabelSet extends Serializable {
  var replaces = Seq[(Array[String], String)]()

  def evalFix(morphString: String): String = {
    var result = morphString

    result = result.replaceAll("Adv", "Adverb")


    result
  }

  def enrichTags(morphString: String, default: String = "NONE"): String = {

    var result: String = default
    val loop = new Breaks
    loop.breakable {
      for (replace <- replaces) {
        val search = replace._1
        val replacement = replace._2
        var indices = Array[Int]()

        for (i <- search.length - 1 to 0 by -1) {
          val searchTag = search(i)
          val indice = morphString.lastIndexOf(searchTag)
          indices = indices :+ indice
        }

        if (checkIndice(indices)) {
          result = replacement
          loop.break()
        }

      }
    }
    result
  }

  protected def checkIndice(indices: Array[Int]): Boolean = {
    var matching = true
    val loop = new Breaks

    if (indices.exists(p => p == -1)) {
      matching = false
    }
    else if (indices.length > 1) {
      loop.breakable {
        for (i <- 0 until indices.length - 1) {
          if (indices(i) < indices(i + 1)) {
            matching = false
            loop.break()
          }
        }
      }
    }



    matching
  }
}

object MorphTagSet extends LabelSet {

  //Case conditions

  replaces = replaces :+ (Array("Pron", "Dat"), "Pron+Dat")
  replaces = replaces :+ (Array("Noun", "Dat"), "Noun+Dat")

  replaces = replaces :+ (Array("Pron", "Loc"), "Pron+Loc")
  replaces = replaces :+ (Array("Noun", "Loc"), "Noun+Loc")

  replaces = replaces :+ (Array("Pron", "Abl"), "Pron+Abl")
  replaces = replaces :+ (Array("Noun", "Abl"), "Noun+Abl")

  replaces = replaces :+ (Array("Pron", "Acc"), "Pron+Acc")
  replaces = replaces :+ (Array("Noun", "Acc"), "Noun+Acc")

  replaces = replaces :+ (Array("Noun", "Inst"), "Noun+Inst")
  replaces = replaces :+ (Array("Pron", "Inst"), "Pron+Inst")

  replaces = replaces :+ (Array("Noun", "Gen"), "Noun+Gen")
  replaces = replaces :+ (Array("Pron", "Gen"), "Pron+Gen")

  replaces = replaces :+ (Array("Dat"), "Dat")
  replaces = replaces :+ (Array("Loc"), "Loc")
  replaces = replaces :+ (Array("Abl"), "Abl")
  replaces = replaces :+ (Array("Acc"), "Acc")
  replaces = replaces :+ (Array("Inst"), "Inst")
  replaces = replaces :+ (Array("Gen"), "Gen")

  replaces = replaces :+ (Array("P3sg"), "P3sg")
  replaces = replaces :+ (Array("P3pl"), "P3pl")
  replaces = replaces :+ (Array("P2sg"), "P2sg")
  replaces = replaces :+ (Array("P2pl"), "P2pl")


  replaces = replaces :+ (Array("Det"), "Det")
  replaces = replaces :+ (Array("Adj"), "Adj")
  replaces = replaces :+ (Array("Adv"), "Adv")
  replaces = replaces :+ (Array("Verb"), "Verb")
  replaces = replaces :+ (Array("Noun"), "Noun")
  replaces = replaces :+ (Array("Pron"), "Pron")
  replaces = replaces :+ (Array("Conj"), "Conj")
  replaces = replaces :+ (Array("Interj"), "Interj")
  replaces = replaces :+ (Array("Num"), "Num")
  replaces = replaces :+ (Array("Punc"), "Punc")
  replaces = replaces :+ (Array("Sym"), "Sym")


  def main(args: Array[String]) {
    println(enrichTags("ev+Noun+A3sg+P3sg+Nom"))
  }

}

object AgreementTagSet extends LabelSet {
  replaces = replaces :+ (Array("A3sg", "P3pl"), "A3sg+P3pl")
  replaces = replaces :+ (Array("A3sg", "P3sg"), "A3pl+P3sg")
  replaces = replaces :+ (Array("A3pl", "P3pl"), "A3pl+P3pl")
  replaces = replaces :+ (Array("A3pl", "P3sg"), "A3sg+P3sg")


  replaces = replaces :+ (Array("Noun", "A3sg"), "Noun+A3sg")
  replaces = replaces :+ (Array("Noun", "A3pl"), "Noun+A3pl")

  replaces = replaces :+ (Array("A1sg"), "A1sg")
  replaces = replaces :+ (Array("A1pl"), "A1pl")

  replaces = replaces :+ (Array("P2sg"), "P2sg")
  replaces = replaces :+ (Array("P2pl"), "P2pl")
  replaces = replaces :+ (Array("P3sg"), "P3sg")
  replaces = replaces :+ (Array("P3pl"), "P3pl")

}

object CaseTagSet extends LabelSet() {
  replaces = replaces :+ (Array("Pnon", "Nom"), "Pnon+Nom")
  replaces = replaces :+ (Array("Pnon", "Dat"), "Pnon+Dat")
  replaces = replaces :+ (Array("Pnon", "Loc"), "Pnon+Loc")
  replaces = replaces :+ (Array("Pnon", "Abl"), "Pnon+Abl")
  replaces = replaces :+ (Array("Pnon", "Acc"), "Pnon+Acc")
  replaces = replaces :+ (Array("Pnon", "Inst"), "Pnon+Inst")
  replaces = replaces :+ (Array("Pnon", "Gen"), "Pnon+Gen")

  replaces = replaces :+ (Array("Nom"), "Nom")
  replaces = replaces :+ (Array("Dat"), "Dat")
  replaces = replaces :+ (Array("Loc"), "Loc")
  replaces = replaces :+ (Array("Abl"), "Abl")
  replaces = replaces :+ (Array("Acc"), "Acc")
  replaces = replaces :+ (Array("Inst"), "Inst")
  replaces = replaces :+ (Array("Gen"), "Gen")

}

object PosTagSet extends LabelSet {
  replaces = replaces :+ (Array("Noun", "Prop"), "Noun+Prop")
  replaces = replaces :+ (Array("Pron", "Reflex"), "Pron+Reflex")
  replaces = replaces :+ (Array("Pron", "Demons"), "Pron+Demons")
  replaces = replaces :+ (Array("Adverb", "ByDoingSo"), "Adverb+ByDoingSo")


  replaces = replaces :+ (Array("Det"), "Det")
  replaces = replaces :+ (Array("Num"), "Num")
  replaces = replaces :+ (Array("Noun"), "Noun")
  replaces = replaces :+ (Array("Interj"), "Interj")
  replaces = replaces :+ (Array("Conj"), "Conj")
  replaces = replaces :+ (Array("Postp"), "Postp")
  replaces = replaces :+ (Array("Verb"), "Verb")
  replaces = replaces :+ (Array("Adj"), "Adj")
  replaces = replaces :+ (Array("Adverb"), "Adverb")
  replaces = replaces :+ (Array("Pron"), "Pron")

  replaces = replaces :+ (Array("Punc"), "Punc")
  replaces = replaces :+ (Array("Ques"), "Ques")
  replaces = replaces :+ (Array("Dup"), "Dup")

}

object DerivationTagSet extends LabelSet{
  replaces = replaces :+ (Array("Verb^DB", "Verb"), "Verb^DB+Verb")
  replaces = replaces :+ (Array("Verb", "Neg^DB"), "Verb+Neg^DB")
  replaces = replaces :+ (Array("Pos^DB", "Noun"), "Pos^DB+Noun")
  replaces = replaces :+ (Array("Pos^DB", "Adj"), "Pos^DB+Adj")
  replaces = replaces :+ (Array("Pos^DB", "Adverb"), "Pos^DB+Adverb")

}

object VerbTagSet extends LabelSet {

  //Tenses

  replaces = replaces :+ (Array("Verb", "FutPart"), "Verb+FutPart")
  replaces = replaces :+ (Array("Verb", "Prog1"), "Verb+Prog1")
  replaces = replaces :+ (Array("Adv", "When"), "Adv+When")
  replaces = replaces :+ (Array("Pos", "Past"), "Pos+Past")


  replaces = replaces :+ (Array("Verb", "Aor"), "Verb+Aor")
  replaces = replaces :+ (Array("Verb", "Neg"), "Verb+Neg")
  replaces = replaces :+ (Array("Verb", "Neces"), "Verb+Neces")
  replaces = replaces :+ (Array("Verb", "Cond"), "Verb+Cond")
  replaces = replaces :+ (Array("Verb", "Pass"), "Verb+Pass")
  replaces = replaces :+ (Array("Verb", "Caus"), "Verb+Caus")


  replaces = replaces :+ (Array("Past"), "Past")
  replaces = replaces :+ (Array("Verb"), "Verb")


}


object PennPosTagSet extends LabelSet {


  replaces = replaces :+ (Array("Det"), "DT")

  replaces = replaces :+ (Array("Num", "Card"), "CD")
  replaces = replaces :+ (Array("Num", "Ord"), "CD")
  replaces = replaces :+ (Array("Num"), "CD")

  replaces = replaces :+ (Array("Noun", "pl"), "NNS")
  replaces = replaces :+ (Array("Noun", "sg"), "NN")
  replaces = replaces :+ (Array("Noun"), "NN")

  replaces = replaces :+ (Array("Interj"), "UH")
  replaces = replaces :+ (Array("Conj"), "CC")
  replaces = replaces :+ (Array("Postp"), "POSTP")

  //???
  replaces = replaces :+ (Array("Verb", "Past"), "VBD")
  replaces = replaces :+ (Array("Verb", "Narr"), "VBD")

  replaces = replaces :+ (Array("Verb", "Pos"), "VBG")
  replaces = replaces :+ (Array("Verb", "PresPart"), "VBG")

  //???? VBN = has been
  replaces = replaces :+ (Array("Verb", "Past"), "VBN")
  replaces = replaces :+ (Array("Verb", "Pres", "A3sg"), "VBZ")
  replaces = replaces :+ (Array("Verb", "Pres"), "VBP")
  replaces = replaces :+ (Array("Verb"), "VB")

  replaces = replaces :+ (Array("Adj", "Without"), "JJWOUT")
  replaces = replaces :+ (Array("Adj", "With"), "JJWITH")
  replaces = replaces :+ (Array("Adj", "PastPart"), "JJPAST")
  replaces = replaces :+ (Array("Adj", "PresPart"), "JJPAST")
  replaces = replaces :+ (Array("Adj"), "JJ")

  replaces = replaces :+ (Array("Adv"), "RB")

  replaces = replaces :+ (Array("Pron", "Pers"), "PRP")
  replaces = replaces :+ (Array("Pron", "Gen"), "PRP$")
  replaces = replaces :+ (Array("Pron", "Ques", "Gen"), "WRP$")
  replaces = replaces :+ (Array("Pron", "Ques"), "WRP")
  replaces = replaces :+ (Array("Pron", "Reflex"), "REFPRP")
  replaces = replaces :+ (Array("Pron"), "PRON")

  replaces = replaces :+ (Array("Punc"), "SYM")
  replaces = replaces :+ (Array("Ques"), "QUES")
  replaces = replaces :+ (Array("Dup"), "DUP")

  replaces = replaces :+ (Array("NONE"), "NN")
  replaces = replaces :+ (Array("A3sg"), "NN")
  replaces = replaces :+ (Array("Sym"), "SYM")


  def main(args: Array[String]) {
    println(PennPosTagSet.enrichTags("kendi+Pron+Reflex+A3sg+P3sg+Nom"))
  }

}