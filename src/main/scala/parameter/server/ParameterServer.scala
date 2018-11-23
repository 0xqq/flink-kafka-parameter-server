package parameter.server

import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import parameter.server.communication.Messages.Message
import parameter.server.logic.worker.WorkerLogic
import parameter.server.utils.RedisPubSubSource
import parameter.server.utils.Types.{Parameter, ParameterServerOutput, WorkerInput}

class ParameterServer[T <: WorkerInput,
                      P <: Parameter,
                      WK, SK](
                               env: StreamExecutionEnvironment,
                               src: DataStream[T],
                               workerLogic: WorkerLogic[WK, SK, T, P],
                               serverToWorkerParse: String => Message[SK, WK, P]//, workerToServerParse: String => Message[WK, SK, P],
                             ) {

  def start(): DataStream[ParameterServerOutput] = {
    init()
    workerStream(
      workerInput(
        src, serverToWorker())
    )
  }

  def init(): Unit = { }

  def serverToWorker(): DataStream[Message[SK, WK, P]] =
    env
      .addSource(new RedisPubSubSource())//(serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerParse)

  def workerInput(inputStream: DataStream[T], serverToWorkerStream: DataStream[Message[SK, WK, P]]): ConnectedStreams[Message[SK, WK, P], T] = {
//    if (broadcastServerToWorkers)
//      serverToWorkerStream.broadcast
//        .connect(inputStream.keyBy(_.destination.hashCode()))
//    else
      serverToWorkerStream
        .connect(inputStream)
        .keyBy(_.destination.hashCode(), _.destination.hashCode())
  }

  def workerStream(workerInputStream: ConnectedStreams[Message[SK, WK, P], T]): DataStream[ParameterServerOutput] =
    workerInputStream
      .flatMap(workerLogic)


}
