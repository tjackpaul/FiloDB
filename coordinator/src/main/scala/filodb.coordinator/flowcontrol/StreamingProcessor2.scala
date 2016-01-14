package filodb.coordinator.flowcontrol

import akka.actor.FSM
import filodb.coordinator.reactive._
import filodb.core.util.FiloLogging

import scala.concurrent.duration.{Duration, FiniteDuration}

sealed trait State

case object Idle extends State

case object Waiting extends State

case object Processing extends State

case object Ended extends State

sealed trait Data

case object Data extends Data

class StreamingProcessor2[Input, Output](val iterator: Iterator[Input],
                                         chunkProcessor: Iterator[Input] => Output,
                                         backPressureCheck: () => Boolean,
                                         onSuccess: (Int) => Unit,
                                         onFailure: (Int, FlowControlMessage) => Unit,
                                         chunkSize: Int,
                                         maxRetries: Int,
                                         initialBackoff: Duration)
  extends FSM[State, Data] with FiloLogging{

  var retries = 0
  var totalProcessed = 0
  var currentBackoff = initialBackoff
  val chunkIterator = iterator.grouped(chunkSize)


  startWith(Idle, Data)

  when(Idle) {
    case Event(StartFlow,_) =>
      goto(Processing)
  }

  when(Processing) {
    case Event(Acknowledged(size),_) =>
      totalProcessed = totalProcessed + size
      metrics.warn(s"Total processed $totalProcessed")
      if (chunkIterator.hasNext) {
        goto(Processing)
      } else {
        onSuccess(totalProcessed)
        stop()
      }

    case Event(Backoff(duration),_) =>
      metrics.warn(s"Back pressure backoff for duration$duration")
      // back off for duration and start flow again after that
      setTimer("Backoff", StartFlow, FiniteDuration(duration.length, duration.unit))
      goto(Idle)

    case Event(MaxRetriesExceeded(someRetries),_) =>
      metrics.warn(s"Max retries $someRetries exceeded")
      stop(FSM.Failure(s"Max retries $someRetries exceeded"))

  }

  onTransition {
    case _ -> Processing =>
      if (retries < maxRetries) {
        if (backPressureCheck()) {
          val chunk = chunkIterator.next
          val size = chunk.size
          chunkProcessor(chunk.iterator)
          // we are back processing again. So reset max retries
          retries = maxRetries
          self ! Acknowledged(size)
        } else {
          // back off exponentially until max retries
          self ! Backoff(initialBackoff * Math.pow(2d, retries))
        }
      } else {
        self ! MaxRetriesExceeded(maxRetries)
      }

  }


}
