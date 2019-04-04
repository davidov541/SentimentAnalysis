package net.davidmcginnis.flink.twittersentiment

import java.util.{Properties, StringTokenizer}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.mutable.ListBuffer
import scala.collection.convert.wrapAll._

/**
  * Implements the "TwitterStream" program that computes a most used word
  * occurrence over JSON objects in a streaming fashion.
  *
  * The input is a Tweet stream from a TwitterSource.
  *
  * Usage:
  * {{{
  * TwitterExample [--output <path>]
  * }}}
  *
  */
object TwitterSentiment {

  private val CONSUMER_KEY = "eDkMMlSI6uHm8tGKNZnEbpu6A"
  private val CONSUMER_SECRET = "0viC9t2pXN09e2L1dcJhVi5CPSm0dMSxaJXEljWaFUTBYA17ra"
  private val TOKEN = "944270801494269952-blMQApSk9tCxglGzCIcvaBn0PjANyJ2"
  private val TOKEN_SECRET = "QWJ5yWAem4iTTQyCiPCOdAZjBLjvxQTByPXM9kqsKSCAK"
  private val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  private val pipeline = new StanfordCoreNLP(props)

  def main(args: Array[String]): Unit = {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    env.setParallelism(params.getInt("parallelism", 1))

    // get input data
    val properties = new Properties()
    properties.setProperty(TwitterSource.CONSUMER_KEY, CONSUMER_KEY)
    properties.setProperty(TwitterSource.CONSUMER_SECRET, CONSUMER_SECRET)
    properties.setProperty(TwitterSource.TOKEN, TOKEN)
    properties.setProperty(TwitterSource.TOKEN_SECRET, TOKEN_SECRET)
    val source = new TwitterSource(properties)
    source.setCustomEndpointInitializer(new HashtagFilterEndpointInitializer("thelastjedi"))
    val streamSource: DataStream[String] = env.addSource(source)

    lazy val jsonParser = new ObjectMapper()

    val tweets = streamSource.
      filter { json =>
        val jsonNode = jsonParser.readValue(json, classOf[JsonNode])
        jsonNode.has("text") && !jsonNode.get("text").textValue().startsWith("RT") && jsonNode.has("user") &&
        jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText == "en"
      }.map{ json =>
        val jsonNode = jsonParser.readValue(json, classOf[JsonNode])
        jsonNode.get("text").textValue()
      }.map(extractSentiment(_)).filter(_._3).map(x => (x._1, x._2))

    val sentimentAggregation = tweets.map(x => ("SentimentCounts", Map((x._2, 1)))).keyBy(_._1).reduce((x, y) => {
      val origMap = x._2
      val newMap = y._2.foldLeft(origMap){ case (map, s1) =>
        val oldValue = map.getOrElse(s1._1, 0)
        map.updated(s1._1, oldValue + s1._2)
      }
      (x._1, newMap)
    }).map(_._2)

    // emit result
    if (params.has("output")) {
      tweets.writeAsText(params.get("output"))
      sentimentAggregation.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      tweets.print()
      sentimentAggregation.print()
    }

    // execute program
    env.execute("Twitter Streaming Example")
  }

  private def extractSentiment(text: String) : (String, Int, Boolean) = {
    val sentiments = extractSentiments(text)
    if (sentiments.isEmpty) ("<NO TWEET>", 0, false)
    else (text, getMean(sentiments).intValue, true)
  }

  private def getMean(sentiments : List[(String, Int)]) : Double = {
    val sum = sentiments.map(_._2).sum.doubleValue()
    sum / sentiments.length.doubleValue()
  }

  def extractSentiments(text: String): List[(String, Int)] = {
    val annotation: Annotation = pipeline.process(text.filter(c => c.toInt < 255))
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, RNNCoreAnnotations.getPredictedClass(tree) + 1) }
      .toList
  }

  /**
    * Deserialize JSON from twitter source
    *
    * Implements a string tokenizer that splits sentences into words as a
    * user-defined FlatMapFunction. The function takes a line (String) and
    * splits it into multiple pairs in the form of "(word,1)" ({{{ Tuple2<String, Integer> }}}).
    */
  private class SelectEnglishAndTokenizeFlatMap extends FlatMapFunction[String, (String, Int)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
      // deserialize JSON from twitter source
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val isEnglish = jsonNode.has("user") &&
        jsonNode.get("user").has("lang") &&
        jsonNode.get("user").get("lang").asText == "en"
      val hasText = jsonNode.has("text")

      (isEnglish, hasText, jsonNode) match {
        case (true, true, node) => {
          val tokens = new ListBuffer[(String, Int)]()
          val tokenizer = new StringTokenizer(node.get("text").asText())

          while (tokenizer.hasMoreTokens) {
            val token = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase()
            if (token.nonEmpty)out.collect((token, 1))
          }
        }
        case _ =>
      }
    }
  }
}