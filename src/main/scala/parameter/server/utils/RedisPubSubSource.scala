package parameter.server.utils

import com.redis._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

class RedisPubSubSource extends RichSourceFunction[String] {

  var client: RedisClient = _
  var channelName: String = _
  var sourceContext: SourceContext[String] = _

  override def open(parameters: Configuration) = {
    client = new RedisClient(parameters.getString("host", "localhost"), parameters.getInteger("port", 6379))
    channelName = parameters.getString("channel", "uservectors")
  }

  @volatile var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
    client.disconnect
  }

  def msgConsumeCallback(msg: PubSubMessage) = {
    msg match {
      case S(channel, _) => //TODO log subscribe
      case U(channel, _) => //TODO log unsubscribe
      case M(channel, msgContent) => sourceContext.collect(msgContent)
      case E(throwable) =>  //TODO log error
    }
  }

  override def run(ctx: SourceContext[String]): Unit = while (isRunning) {
    sourceContext = ctx
    client.subscribe(channelName)(msgConsumeCallback(_))
    //for {
    //  data <- client. get data ...
    //} yield ctx.collect(data)
  }

}
