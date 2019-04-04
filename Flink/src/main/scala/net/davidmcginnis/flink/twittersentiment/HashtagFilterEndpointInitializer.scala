package net.davidmcginnis.flink.twittersentiment

import scala.collection.JavaConversions._

import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StreamingEndpoint}
import org.apache.flink.streaming.connectors.twitter.TwitterSource.EndpointInitializer

class HashtagFilterEndpointInitializer(private val _hashtag : String) extends EndpointInitializer with Serializable {
  override def createEndpoint(): StreamingEndpoint = {
    val endpoint = new StatusesFilterEndpoint()
    endpoint.trackTerms(List.apply("#" + _hashtag))
    endpoint
  }
}
