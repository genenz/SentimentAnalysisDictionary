/**
  * Created by gene on 1/18/16.
  */
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object SentimentAnalysisDictionary {

  // Program Inputs
  val consumerKey = "YOUR_TWITTER_CONSUMER_KEY"
  val consumerSecret = "YOUR_TWITTER_CONSUMER_SECRET"
  val accessToken = "YOUR_TWITTER_ACCESS_TOKEN"
  val accessTokenSecret = "YOUR_TWITTER_ACCESS_TOKEN_SECRET"
  val filters = Array("TWITTER_FILTER_TERMS")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SentimentAnalysisDictionary")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    // Define the Twitter Connection details so twitter4j can connect.
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Create the DStream that finds tweets based on a filter
    val tweetStream = TwitterUtils.createStream(ssc,None,filters)

    // Create the Sentiment Dictionary RDD based on the AFINN Dictionary.
    // Note: The AFINN dictionary rates each word based on a number between -5 and +5.
    // + indicates positive sentiment.  - indicates negative sentiment.
    val source = getClass.getResource("/AFINN-111.txt")
    val sentimentDictionary = sc.textFile(source.toString,1)

    // Convert sentimentDictionary into a group of tuples
    val sentDictPairedRDD = sentimentDictionary.map(line => {
      val splitLine = line.split("\t")
      (splitLine(0), splitLine(1).toLong)
    })
    sentDictPairedRDD.cache()   // Cache the RDD as we will reuse regularly

    // Take each stream of tweets (as measured every 2 seconds).
    // Break them down to individual words and 'measure' based on value of the words
    // against the corresponding dictionary
    tweetStream.foreachRDD( tweetRDD => {

      val origTweets = tweetRDD.collect()   // Save the original tweets so we can reference them later.

      // Begin breaking down each tweet by removing noise such as punctuation and capitalisation
      val cleanedTweets = tweetRDD.map(tweet => tweet.getText.toLowerCase().replaceAll("""[\p{Punct}]""", ""))

      // Give an index value to each tweet based on their position in the RDD.
      // Then break down each tweet into its constituent words with the index associated with it.
      // We create (word,index) tuples from this.
      val cleanedAndIndexedTweets = cleanedTweets.zipWithIndex()
      val tweetsIndexedAsWordPairs = cleanedAndIndexedTweets.flatMap( {case (tweet, index) => {
        val tweetWords = tweet.split(" ")
        tweetWords.map(word => (word, index))
      }})

      // Here we join the individual words with the dictionary.  This allows us to associate a sentiment value
      // based on the words that make up the tweet.  Because we no longer care about the word itself
      // we get rid of the associated word and keep the tweet index and sentiment value for each word in a tweet
      val individualWordSentimentPerTweet = tweetsIndexedAsWordPairs.join(sentDictPairedRDD).values

      // Now add up the value of each index to get a final sentiment value of the tweet.
      val indexTweetSentiment = individualWordSentimentPerTweet.reduceByKey((a, b) => a + b)

      // Create an output tuple associating the original tweet with the final sentiment value.
      // If the sentiment value is positive, then it represents positive sentiment.  Negative represents negative sentiment etc.
      val finalSentiments = indexTweetSentiment.map( {case (index, value) =>
        (origTweets(index.toInt).getText, if(value < 0) "negative"; else if (value > 0) "positive"; else "neutral")
      })

      // Print to screen the results.
      finalSentiments.foreach( { case (index, value) => println(s"($index, $value)")})
    })

    ssc.start()
    ssc.awaitTermination()
  }
}