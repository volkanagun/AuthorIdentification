package language.postagger

import java.io._
import java.net.URI

import cc.mallet.fst._
import cc.mallet.optimize.Optimizable
import cc.mallet.pipe.Pipe
import cc.mallet.types._
import classification.postagging.{CaseTagSet, PennLabelSet, SelectedTagSet}
import data.document.Sentence
import language.morphology._
import language.tokenization.TokenizerImp
import options.Resources
import options.Resources.MorphologyResources
import org.apache.spark.ml.{Context, Window}


/**
  * Created by wolf on 14.04.2016.
  */
class PosTaggerCRF(val labelSet:LabelSet, val modelFile:String) extends PosTaggerImp {
  //Mallet trainer
  val context = new Window[String](3)
  val pipe = new PosTaggerPipe(labelSet, context)
  val modelFilename = modelFile
  lazy val classifier: CRF = loadCRF(modelFilename)
  pipe.setTargetProcessing(true)

  def loadCRF(filename: String): CRF = {
    if (new File(filename).exists()) {
      //println("Loading PosTaggerCRF model...")
      val in = new ObjectInputStream(new FileInputStream(filename))
      val crf = in.readObject().asInstanceOf[CRF]
      in.close()
      //println("Loaded filename : "+filename)
      crf
    }
    else {
      null
    }
  }

  def saveCRF(filename: String, crf: CRF): Unit = {

    val out = new ObjectOutputStream(new FileOutputStream(filename))
    out.writeObject(crf)
    out.close()
  }

  protected def instance(tokens: Seq[MorphLight], id: Int): Instance = {
    val uri = new URI(id.toString)
    val tokenLabels = tokens.map(morph => {
      (morph.token, morph.label)
    })
    new Instance(tokenLabels, null, uri, null)
  }

  protected def instance(tokens: Seq[MorphLight], uri: URI): Instance = {
    val tokenLabels = tokens.map(morph => {
      (morph.token, morph.label)
    })
    new Instance(tokenLabels, null, uri, null)
  }

  protected def instance(tokens: Seq[String]): Instance = {
    val uri = new URI("fix")
    val tokenLabels = tokens.map(token => (token, "UNK"))
    new Instance(tokenLabels, null, uri, null)
  }

  protected def instances(sentences: Seq[(Int, String, Seq[MorphLight])]): InstanceList = {
    val instanceList = new InstanceList(pipe)
    sentences.foreach(sentence => {
      val uri = new URI(sentence._1.toString)
      val myinstance = instance(sentence._3, uri)
      instanceList.addThruPipe(myinstance)
    })

    instanceList
  }

  override def testTokens(tokens: Seq[String]): Seq[(String, String)] = {
    testTokens(classifier, tokens)
  }

  override def test(tokens:Seq[String]):Seq[String]={
   this.synchronized {
      test(classifier, tokens)
    }
  }

  def testTokens(tagger:CRF, tokens: Seq[String]): Seq[(String, String)] = {
    val myInstance = instance(tokens)

    val edInstance = this.synchronized(tagger.transduce(myInstance))
    val labels = edInstance.getData.asInstanceOf[ArraySequence[String]]

    tokens.zipWithIndex.map { case (token, indice) => {
      (token, labels.get(indice))
    }}
  }

  def test(tagger:CRF, tokens: Seq[String]): Seq[String] = {
    val myInstance = instance(tokens)

    val edInstance = this.synchronized {
      tagger.transduce(myInstance)
    }

    val labels = edInstance.getData.asInstanceOf[ArraySequence[String]]
    for(i<-0 until labels.size()) yield labels.get(i)

  }



  def train(sentences: Seq[(Int, String, Seq[MorphLight])], numThreads: Int = 24): CRF = {
    val instanceList = instances(sentences)
    val crf = new CRF(pipe, null)

    crf.addStartState()

    crf.addFullyConnectedStatesForLabels()
    //Cut off speed but may be useful
    //crf.addFullyConnectedStatesForBiLabels()
    //crf.addFullyConnectedStatesForTriLabels()

    crf.setWeightsDimensionAsIn(instanceList, true)
    val batchOptLabel = new CRFOptimizableByBatchLabelLikelihood(crf, instanceList, numThreads);
    val optLabel = new ThreadedOptimizable(batchOptLabel, instanceList, crf.getParameters().getNumFactors(),
      new CRFCacheStaleIndicator(crf));

    val opts = Array[Optimizable.ByGradientValue](optLabel);
    val crfTrainer = new CRFTrainerByValueGradients(crf, opts)
    crfTrainer.train(instanceList, Integer.MAX_VALUE)
    optLabel.shutdown()
    val trainedCRF = crfTrainer.getCRF
    saveCRF(modelFilename, trainedCRF)
    trainedCRF
  }

  def evaluate(training: Seq[(Int, String, Seq[MorphLight])], test: Seq[(Int, String, Seq[MorphLight])], numThreads: Int = 24): Unit = {
    val trainList = instances(training)
    val testList = instances(test)

    val crf = new CRF(trainList.getDataAlphabet, trainList.getTargetAlphabet)

    crf.addFullyConnectedStatesForLabels()
    //crf.addFullyConnectedStatesForBiLabels()
    //crf.addFullyConnectedStatesForTriLabels()

    crf.setWeightsDimensionAsIn(trainList, false)
    val batchOptLabel = new CRFOptimizableByBatchLabelLikelihood(crf, trainList, numThreads);
    val optLabel = new ThreadedOptimizable(batchOptLabel, trainList, crf.getParameters().getNumFactors(),
      new CRFCacheStaleIndicator(crf));

    val opts = Array[Optimizable.ByGradientValue](optLabel);
    val crfTrainer = new CRFTrainerByValueGradients(crf, opts)
    //val crfTrainer =  new CRFTrainerByL1LabelLikelihood(crf, 0.05)
    val labels = testList.getTargetAlphabet.toArray
    val evaluator = new MultiSegmentationEvaluator(Array[InstanceList](trainList, testList), Array("train", "test"), labels, labels) {
      override def precondition(tt: TransducerTrainer): Boolean = {
        tt.getIteration % 25 == 0
      }
    }

    val tokenAccuracyEvaluator = new ConfusionEvaluator(trainList, testList)
    val instanceAccuracyEvaluator = new InstanceAccuracyEvaluator()

    //crfTrainer.addEvaluator(evaluator)
    //crfTrainer.addEvaluator(tokenAccuracyEvaluator)
    //crfTrainer.addEvaluator(instanceAccuracyEvaluator)

    crfTrainer.train(trainList, Integer.MAX_VALUE)
    optLabel.shutdown()

    evaluator.evaluate(crfTrainer)
    tokenAccuracyEvaluator.evaluateInstanceList(crfTrainer, testList, "test")
    instanceAccuracyEvaluator.evaluateInstanceList(crfTrainer, testList, "test")

    println("STARTING FINAL EVALUATION...");
    println("Micro Avearge: " + tokenAccuracyEvaluator.getAccuracy("test"));
    println("Macro Avearge: " + instanceAccuracyEvaluator.getAccuracy("test"));
    tokenAccuracyEvaluator.printConfusion()
  }

}

object PosTaggerCRF {
  val trainBigFilename = Resources.MorphologyResources.sentenceMorphTrainLarge
  val trainFilename = Resources.MorphologyResources.sentenceMorphTrainDev
  val testFilename = Resources.MorphologyResources.sentenceMorphTest

  val trainBigData = Resources.MorphologyResources.loadMorphology(trainBigFilename)
  val trainData = Resources.MorphologyResources.loadMorphology(trainFilename)
  val testData = Resources.MorphologyResources.loadMorphology(testFilename)
  val posTaggerCRF = new PosTaggerCRF(PennPosTagSet, MorphologyResources.crfPennPosTag)

  def main(args: Array[String]) {

    //val classifier = posTaggerCRF.train(trainData,24)
    val annotation = posTaggerCRF.testTokens(Seq("Ali","kendini", "gösterdi",".")).map(pair=>pair._1+"/"+pair._2).mkString(" ")
    println(annotation)

    //posTaggerCRF.evaluate(testData, trainData, 24)
  }
}

class PosTaggerPipe(val labelset:LabelSet, val context: Window[String]) extends Pipe(new Alphabet(), new LabelAlphabet()) {


  protected def extract(instance: Instance, targetProcessing: Boolean = false): Instance = {
    val tokenLabels: Seq[(String, String)] = instance.getData.asInstanceOf[Seq[(String, String)]]
    val tokens = tokenLabels.map(tokenLabel => (tokenLabel._1, labelset.enrichTags(tokenLabel._2)))
    val features = PosTaggerFeatures.generate(context, tokens)
    val tokenLength = tokens.length
    var fvs = Array[FeatureVector]();
    val labelAlphabet = if (targetProcessing) getTargetAlphabet().asInstanceOf[LabelAlphabet] else null
    val featureAlphabet = getDataAlphabet()
    val targetSequence = new LabelSequence(labelAlphabet, tokens.length)

    for (i <- 0 until tokenLength) {
      val featureSeq = features(i)
      val nfeatureSeq = featureSeq._2
      var nlabel = tokens(i)._2

      if (targetProcessing) {
        targetSequence.add(nlabel)
      }

      val indiceArr = nfeatureSeq.map(key => {
        featureAlphabet.lookupIndex(key)
      })
        .filter(indice => indice >= 0)
        .toArray

      fvs = fvs :+ new FeatureVector(featureAlphabet, indiceArr)

    }

    instance.setData(new FeatureVectorSequence(fvs))
    if(targetProcessing) {
      instance.setTarget(targetSequence)
    }
    instance
  }

  override def pipe(instance: Instance): Instance = {
    extract(instance, isTargetProcessing)
  }


}

object PosTaggerFeatures {

  val context = new Context()

  def generate(window: Window[String], tokens: Seq[(String, String)]): Array[(String, Seq[String])] = {
    //generate each token features
    //token and array of features
    val features = context.tokenLabelFeatures(tokens)
    val wfeatures = window.window(features)
    wfeatures.map(pair => {
      (pair._3, pair._5.distinct)
    }).toArray
    //features.map(pair=>{(pair._3,pair._4.distinct)}).toArray
  }
}

class ConfusionEvaluator(trainList: InstanceList, testList: InstanceList) extends TokenAccuracyEvaluator(Array(trainList, testList), Array("train", "test")) {
  var accuracy = Map[String, Double]()
  var confusion = Map[String, Double]()
  var count = Map[String, Double]()

  override def evaluateInstanceList(trainer: TransducerTrainer, instances: InstanceList, description: String): Unit = {
    var numCorrectTokens: Int = 0
    var totalTokens: Int = 0

    val transducer: Transducer = trainer.getTransducer

    var i: Int = 0
    while (i < instances.size) {
      {
        val instance: Instance = instances.get(i)
        val input: Sequence[_] = instance.getData.asInstanceOf[Sequence[_]]
        val trueOutput: Sequence[_] = instance.getTarget.asInstanceOf[Sequence[_]]
        assert((input.size == trueOutput.size))
        val predOutput: Sequence[_] = transducer.transduce(input)
        assert((predOutput.size == trueOutput.size))
        var j: Int = 0
        while (j < trueOutput.size) {
          {
            val trueLabel = trueOutput.get(j).asInstanceOf[String]
            val predictedLabel = predOutput.get(j).asInstanceOf[String]
            totalTokens += 1
            if (trueLabel.equals(predictedLabel)) ({
              numCorrectTokens += 1;
            })

            val confused = trueLabel + "-->" + predictedLabel
            confusion = confusion.updated(confused, confusion.getOrElse(confused, 0d) + 1d)
            count = count.updated(trueLabel, count.getOrElse(trueLabel, 0d) + 1d)

          }
          ({
            j += 1;
            j - 1
          })
        }
      }
      ({
        i += 1;
        i - 1
      })
    }
    accuracy = accuracy.updated(description, (numCorrectTokens.toDouble) / totalTokens)
    //System.err.println ("TokenAccuracyEvaluator accuracy="+acc);


  }

  override def getAccuracy(description: String): Double = {
    accuracy.getOrElse(description, 0d)
  }

  def printConfusion(): Unit = {
    println("Confusion matrix")
    val mapping = confusion.map { case (label, sum) => {
      val index = label.indexOf("-->")
      val correctLabel = label.substring(0, index)
      val predictedLabel = label.substring(index + 3)
      (correctLabel, predictedLabel, sum / count.getOrElse(correctLabel, 1d))
    }
    }.toArray.sortBy(pair => (pair._1, pair._2))

    mapping.foreach { case (correct, predicted, percent) => {
      println("Correct: " + correct + " Predicted: " + predicted + " Percent:" + percent)
    }
    }

  }
}