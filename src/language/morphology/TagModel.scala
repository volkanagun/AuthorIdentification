package language.morphology

/**
  * Created by wolf on 17.01.2016.
  */
trait MapRule extends Serializable {

  def map(tag:String):String
  def matches(tag:String):Boolean
}

class RuleTag(val rmatch:String,val target:String) extends MapRule {

  override def map(tag: String): String = {
    tag.replaceAll(rmatch, target)
  }

  override def matches(tag: String): Boolean = {
    tag.matches(rmatch)
  }
}

trait TagModel extends Serializable
{
  def mapTag(tag:String):String
}

class EmptyTagModel extends TagModel{
  override def mapTag(tag: String): String = tag
}


class Hasim2TrMorphTagModel extends TagModel{

  var tagrules : Seq[RuleTag] = Seq()

  tagrules = tagrules :+ new RuleTag("Noun","N")
  tagrules = tagrules :+ new RuleTag("Verb","V")
  tagrules = tagrules :+ new RuleTag("Pron","Prn")
  tagrules = tagrules :+ new RuleTag("Demons","dem")
  tagrules = tagrules :+ new RuleTag("Punc","Punc")
  tagrules = tagrules :+ new RuleTag("Det","Det")
  tagrules = tagrules :+ new RuleTag("Conj","Cnj")
  tagrules = tagrules :+ new RuleTag("Num","Num")
  tagrules = tagrules :+ new RuleTag("Adj","Adj")
  tagrules = tagrules :+ new RuleTag("Adv","Adv")
  tagrules = tagrules :+ new RuleTag("Ques","Q")
  tagrules = tagrules :+ new RuleTag("Interj","Ij")
  tagrules = tagrules :+ new RuleTag("Postp","Postp")

  //Cases
  tagrules = tagrules :+ new RuleTag("Ins","ins")
  tagrules = tagrules :+ new RuleTag("Dat","dat")
  tagrules = tagrules :+ new RuleTag("Abl","abl")
  tagrules = tagrules :+ new RuleTag("Acc","acc")
  tagrules = tagrules :+ new RuleTag("Loc","loc")
  tagrules = tagrules :+ new RuleTag("Gen","gen")

  //tagrules = tagrules :+ new RuleTag("Adj+With","cpl")
  tagrules = tagrules :+ new RuleTag("Adj+Without","siz")
  tagrules = tagrules :+ new RuleTag("Adj+FitFor","lik")



  tagrules = tagrules :+ new RuleTag("Pron+Rel","ki")
  tagrules = tagrules :+ new RuleTag("Pers","pers")



  tagrules = tagrules :+ new RuleTag("A1sg","1s")
  tagrules = tagrules :+ new RuleTag("A2sg","2s")
  tagrules = tagrules :+ new RuleTag("A3sg","3s")
  tagrules = tagrules :+ new RuleTag("A1pl","1p")
  tagrules = tagrules :+ new RuleTag("A2pl","2p")
  tagrules = tagrules :+ new RuleTag("A3pl","3p")

  tagrules = tagrules :+ new RuleTag("Neg","neg")
  tagrules = tagrules :+ new RuleTag("Prog1","cont")
  tagrules = tagrules :+ new RuleTag("Past","past")
  tagrules = tagrules :+ new RuleTag("Narr","evid")
  tagrules = tagrules :+ new RuleTag("Verb+Able","abil")
  tagrules = tagrules :+ new RuleTag("Verb+Hastily","iver")
  tagrules = tagrules :+ new RuleTag("Verb+EverSince","agel")
  tagrules = tagrules :+ new RuleTag("Verb+Repeat","adur")
  tagrules = tagrules :+ new RuleTag("Verb+Almost","ayaz")
  tagrules = tagrules :+ new RuleTag("Verb+Stay","akal")
  tagrules = tagrules :+ new RuleTag("Fut","fut")
  tagrules = tagrules :+ new RuleTag("Neces","obl")
  tagrules = tagrules :+ new RuleTag("Prog2","impf")
  tagrules = tagrules :+ new RuleTag("Cond","cond")
  tagrules = tagrules :+ new RuleTag("Opt","opt")
  tagrules = tagrules :+ new RuleTag("Imp","imp")
  tagrules = tagrules :+ new RuleTag("Aor","aor")


  tagrules = tagrules :+ new RuleTag("Adv+AfterDoingSo","cv:ip")
  tagrules = tagrules :+ new RuleTag("Adv+WithoutHavingDoneSo","cv:meksizin")
  tagrules = tagrules :+ new RuleTag("Adv+When","cv:ince")
  tagrules = tagrules :+ new RuleTag("Adv+ByDoingSo","cv:erek")
  tagrules = tagrules :+ new RuleTag("Adv+SinceDoingSo","cv:eli")
  tagrules = tagrules :+ new RuleTag("Adv+AsLongAs","cv:dikce")
  tagrules = tagrules :+ new RuleTag("Noun+FeelLike","cv:esiyle")
  tagrules = tagrules :+ new RuleTag("Adv+AsIf","cv:cesine")
  tagrules = tagrules :+ new RuleTag("Adv+While","cv:ken")




  override def mapTag(tag:String) :String = {
    if(tag==null) return null;

    var tgrep = tag
    tagrules.foreach(rule=>{
      if(rule.matches(tgrep)){
        tgrep = rule.map(tgrep);
      }
    })

    tgrep
  }

}