package net.davidmcginnis.spark.sentimentanalysis

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConverters._
import edu.stanford.nlp.simple.Document
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils

object App {
  private val CONSUMER_KEY = "eDkMMlSI6uHm8tGKNZnEbpu6A"
  private val CONSUMER_SECRET = "0viC9t2pXN09e2L1dcJhVi5CPSm0dMSxaJXEljWaFUTBYA17ra"
  private val TOKEN = "944270801494269952-blMQApSk9tCxglGzCIcvaBn0PjANyJ2"
  private val TOKEN_SECRET = "QWJ5yWAem4iTTQyCiPCOdAZjBLjvxQTByPXM9kqsKSCAK"

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    val context = new StreamingContext(conf, Duration(5000L))
    context.checkpoint("~/Documents/SideProjects/SparkStreamingTwitterSentiment/checkpoints")
    System.setProperty("twitter4j.oauth.consumerKey", CONSUMER_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", CONSUMER_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", TOKEN_SECRET)
    val tweets = TwitterUtils.createStream(context, None, Seq.apply("#thelastjedi"))
    val results = tweets.map(tweet => getSentiment(tweet.getText))
    val stateSpec = StateSpec.function(updateCounts _)
    val counts = results.map(_._2).countByValue()
    counts.map(c => ("sentimentCounts", c)).mapWithState(stateSpec).map(_.get).print()
    context.start()
    context.awaitTermination()
  }

  private def updateCounts(key: String, value: Option[(Int, Long)], state: State[Map[Int, Long]]): Option[Map[Int, Long]] = {
    val count = value.getOrElse((0, 0L))
    val currMap = state.getOption().getOrElse(Map[Int, Long]())
    val currCount = currMap.getOrElse(count._1, 0L)
    val newCount = currCount + count._2
    val newMap = currMap.updated(count._1, newCount)
    state.update(newMap)
    Some(newMap)
  }

  private def getSentiment(tweet : String) : (String, Int) = {
    val doc = new Document(tweet)
    val sentiments = doc.sentences().asScala.map(s => getSentiments(s.text()))
    (tweet, (sentiments.sum.doubleValue() / sentiments.length.doubleValue()).intValue())
  }

  private def getSentiments(tweet : String) : Int = {
    val pipeline = getOrCreateSentimentPipeline()
    val annotation = pipeline.process(tweet).get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
    val tree = annotation.head.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
    RNNCoreAnnotations.getPredictedClass(tree) + 1
  }

  @transient private var _sentimentPipeline: StanfordCoreNLP = _

  private val _sentimentPipelineProperties = new Properties()
  _sentimentPipelineProperties.setProperty("annotators", "tokenize, ssplit, parse, sentiment")

  private def getOrCreateSentimentPipeline(): StanfordCoreNLP = {
    if(_sentimentPipeline == null) _sentimentPipeline = new StanfordCoreNLP(_sentimentPipelineProperties)
    _sentimentPipeline
  }
}
