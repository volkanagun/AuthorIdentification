package classification.postagging

import scala.util.control.Breaks

/**
  * Created by wolf on 11.04.2016.
  */
class PostaggerData {
  //prepare the data, read, write models
  //Map data if necessary
}

class LabelSet {
  var replaces = Seq[(Array[String], String)]()

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

object SelectedTagSet extends LabelSet {


  //Case conditions

  replaces = replaces :+(Array("Pron", "Dat"), "Pron+Dat")
  replaces = replaces :+(Array("Noun", "Dat"), "Noun+Dat")

  replaces = replaces :+(Array("Pron", "Loc"), "Pron+Loc")
  replaces = replaces :+(Array("Noun", "Loc"), "Noun+Loc")

  replaces = replaces :+(Array("Pron", "Abl"), "Pron+Abl")
  replaces = replaces :+(Array("Noun", "Abl"), "Noun+Abl")

  replaces = replaces :+(Array("Pron", "Acc"), "Pron+Acc")
  replaces = replaces :+(Array("Noun", "Acc"), "Noun+Acc")

  replaces = replaces :+(Array("Noun", "Inst"), "Noun+Inst")
  replaces = replaces :+(Array("Pron", "Inst"), "Pron+Inst")

  replaces = replaces :+(Array("Noun", "Gen"), "Noun+Gen")
  replaces = replaces :+(Array("Pron", "Gen"), "Pron+Gen")

  replaces = replaces :+(Array("Dat"), "Dat")
  replaces = replaces :+(Array("Loc"), "Loc")
  replaces = replaces :+(Array("Abl"), "Abl")
  replaces = replaces :+(Array("Acc"), "Acc")
  replaces = replaces :+(Array("Inst"), "Inst")
  replaces = replaces :+(Array("Gen"), "Gen")

  replaces = replaces :+(Array("P3sg"), "P3sg")
  replaces = replaces :+(Array("P3pl"), "P3pl")
  replaces = replaces :+(Array("P2sg"), "P2sg")
  replaces = replaces :+(Array("P2pl"), "P2pl")


  replaces = replaces :+(Array("Det"), "Det")
  replaces = replaces :+(Array("Adj"), "Adj")
  replaces = replaces :+(Array("Adv"), "Adv")
  replaces = replaces :+(Array("Verb"), "Verb")
  replaces = replaces :+(Array("Noun"), "Noun")
  replaces = replaces :+(Array("Pron"), "Pron")
  replaces = replaces :+(Array("Conj"), "Conj")
  replaces = replaces :+(Array("Interj"), "Interj")
  replaces = replaces :+(Array("Num"), "Num")
  replaces = replaces :+(Array("Punc"), "Punc")
  replaces = replaces :+(Array("Sym"), "Sym")


  def main(args: Array[String]) {
    println(enrichTags("ev+Noun+A3sg+P3sg+Nom"))
  }

}

object CaseLabelSet extends LabelSet{
  replaces = replaces :+(Array("Dat"), "Dat")
  replaces = replaces :+(Array("Loc"), "Loc")
  replaces = replaces :+(Array("Abl"), "Abl")
  replaces = replaces :+(Array("Acc"), "Acc")
  replaces = replaces :+(Array("Inst"), "Inst")
  replaces = replaces :+(Array("Gen"), "Gen")
  replaces = replaces :+(Array("Nom"), "Nom")
}

object PennLabelSet extends LabelSet {


  replaces = replaces :+(Array("Det"), "DT")

  replaces = replaces :+(Array("Num", "Card"), "CD")
  replaces = replaces :+(Array("Num", "Ord"), "CD")
  replaces = replaces :+(Array("Num"), "CD")

  replaces = replaces :+(Array("Noun", "pl"), "NNS")
  replaces = replaces :+(Array("Noun", "sg"), "NN")
  replaces = replaces :+(Array("Noun"), "NN")

  replaces = replaces :+(Array("Interj"), "UH")
  replaces = replaces :+(Array("Conj"), "CC")
  replaces = replaces :+(Array("Postp"), "POSTP")

  //???
  replaces = replaces :+(Array("Verb", "Past"), "VBD")
  replaces = replaces :+(Array("Verb", "Narr"), "VBD")

  replaces = replaces :+(Array("Verb", "Pos"), "VBG")
  replaces = replaces :+(Array("Verb", "PresPart"), "VBG")

  //???? VBN = has been
  replaces = replaces :+(Array("Verb", "Past"), "VBN")
  replaces = replaces :+(Array("Verb", "Pres", "A3sg"), "VBZ")
  replaces = replaces :+(Array("Verb", "Pres"), "VBP")
  replaces = replaces :+(Array("Verb"), "VB")

  replaces = replaces :+(Array("Adj", "Without"), "JJWOUT")
  replaces = replaces :+(Array("Adj", "With"), "JJWITH")
  replaces = replaces :+(Array("Adj", "PastPart"), "JJPAST")
  replaces = replaces :+(Array("Adj", "PresPart"), "JJPAST")
  replaces = replaces :+(Array("Adj"), "JJ")

  replaces = replaces :+(Array("Adv"), "RB")

  replaces = replaces :+(Array("Pron", "Pers"), "PRP")
  replaces = replaces :+(Array("Pron", "Gen"), "PRP$")
  replaces = replaces :+(Array("Pron", "Ques", "Gen"), "WRP$")
  replaces = replaces :+(Array("Pron", "Ques"), "WRP")
  replaces = replaces :+(Array("Pron", "Reflex"), "REFPRP")
  replaces = replaces :+(Array("Pron"), "PRON")

  replaces = replaces :+(Array("Punc"), "SYM")
  replaces = replaces :+(Array("Ques"), "QUES")
  replaces = replaces :+(Array("Dup"), "DUP")

  replaces = replaces :+(Array("NONE"), "NN")
  replaces = replaces :+(Array("A3sg"), "NN")
  replaces = replaces :+(Array("Sym"), "SYM")


  def main(args: Array[String]) {
    println(enrichTags("kendi+Pron+Reflex+A3sg+P3sg+Nom"))
  }

}
