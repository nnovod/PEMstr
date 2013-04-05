/*
 * The MIT License
 *
 * Copyright (c) 2013 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sub-license, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.broadinstitute.PEMstr.localScheduler.step.pipe

import akka.actor._
import scala.concurrent.duration._
import StepOutputPipe.{ReadData, PausePipe, ResumePipe}
import scala.concurrent.Future
import java.nio.ByteBuffer
import collection.immutable.Map
import org.broadinstitute.PEMstr.common.util.file.FileInputStreamTracker
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.DataQueue
import util.{Failure, Success}
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler.futureExecutor
import annotation.tailrec
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.BufferSettings

/**
 * @author Nathaniel Novod
 * Date: 9/18/12
 * Time: 3:00 PM
 *
 * Class to handle receiving output data from a step via a pipe.  As data is received we publish the data to the
 * consumers (e.g., a StepOutputSocket).
 *
 * @param consumers map of consumers of data containing (consumer_name -> ActorRef for consumer)
 * @param output Name of output pipe
 * @param buffering settings for buffering to do for stream
 *
 */

class StepOutputPipe(consumers: Map[String, ActorRef], output: String,
                     buffering: BufferSettings) extends Actor with ActorLogging {

	/**
	 * If positive then it's time to pause
 	 */
	private var pause = 0

	/**
	 * Track the output pipe
	 */
	private val inputStream = new FileInputStreamTracker

	/**
	 * Queue to send off request to consumers.  Queue does buffer management and terminates producer (us) and
	 * consumers once the end-of-data has been consumed.
 	 */
	private val dataQueue = new DataQueue(self, consumers, context.system, buffering)

	/**
	 * Remember once if debug is enabled
 	 */
	private val debugEnabled = log.isDebugEnabled

	/**
	 * Message we send to ourselves to start reading on pipe
	 */
	case object StartReading

	/**
	 * Method called before actor starts up - we open up the pipe and read for input.  The open is done in
	 * a separate thread, so that the actor doesn't need to remain active.  When the open completes a message
	 * is sent back to the actor to do a read.
 	 */
	override def preStart() {
		log.info("preStart for output pipe " + output)
		Future {
			inputStream.open(output)
		} onComplete {
			case Success(result) => {
				startReader()
			}
			case Failure(err) => self ! Status.Failure(err)
		}
	}

	/**
	 * Close the stream
 	 */
	override def postStop() {
		log.info("postStop")
		inputStream.close()
	}

	/**
	 * Send ourselves a message to start reading
	 */
	private def startReader() {
		self ! StartReading
	}

	/**
	 * Method called to read data from the pipe.  When we've read the wanted amount of data we send a message back
	 * to the actor with the data read.
	 *
 	 */
	private def read() {
		/**
		 * Little recursive fellow to read till we either each the end of the file or fill buffer to wanted level.
		 *
	 	 * @param stream stream to read data from
		 * @param buf buffer to fill with data
		 *
		 * @return (buffer, end-of-data indicator)
		 */
		@tailrec
		def readData(stream: FileInputStreamTracker, buf: ByteBuffer) : (ByteBuffer, Boolean) = {
			val data = stream.read(buf)
			val (dataBuffer, dataEOF) = data
			if (dataEOF || buffering.isFilled(dataBuffer)) data else readData(stream, dataBuffer)
		}
		/**
		 * If pausing then wait 1/2 second and then say we've read nothing - new ReadData will wind up back here to see
		 * if pause has ended.
		 */
		if (pause > 0) {
			context.system.scheduler.scheduleOnce(500.millisecond) {
				startReader()
			}
		} else {
			/**
			 * Use a future to read the data but don't mess with the data queue outside actor thread
			 */
			val buf = dataQueue.getBuffer
			Future {
				readData(inputStream, buf)
			} onComplete {
				case Success(result) => {
					val (data, eof) = result
					/* We got stuff - go send message to ourselves that the data has arrived */
					self ! ReadData(data, eof)
				}
				case Failure(err) => {
					log.error("Read failed with: " + err.getMessage)
					self ! Status.Failure(err)
				}
			}
		}
	}

	/**
	 * Heart of the actor.  We receive messages and act upon them.
	 */
	def receive = {
		/*
		 * Pause receiving data - we simply keep a count here which is looked at when it comes time to get new data.
		 */
		case PausePipe => {
			log.info("Received pause")
			pause += 1
		}

		/*
		 * Resume receiving data - we simply keep a count here which is looked at when it comes time to get new data.
		 */
		case ResumePipe => {
			log.info("Received resume")
			pause -= 1
		}

		/*
		 * Go read some more data
 		 */
		case StartReading => read()

		/*
		 * Data read - now we publish the data that has been received and start a new thread to read more data if it's
		 * not the end-of-file.
		 *
		 * data: DataBuffer containing data read
		 * eof: true if end-of-file reached
		 */
		case ReadData(data, eof) => {
			if (data.position > 0 || eof) {
				if (data.position > 0) {
					if (debugEnabled)
						log.debug("Publishing " + data.position + " bytes of data for " + output)
					dataQueue.publishData(data, eof)
				} else {
					log.info("Publishing end-of-file for " + output)
					dataQueue.publishEndOfData()
				}
			}
			/* If eof don't do a stop of actor here.  DataQueue will do that once published data is all digested */
			if (!eof) read()
		}

		/*
		 * Unknown message
		 */
		case e => log.warning("received unknown message: " + e)
	}

}

/**
 * Akka message content for output pipe
 */
object StepOutputPipe {

	/**
	 * Base for pipe flow messages
	 */
	sealed trait OutputPipeFlow

	/**
	 * Pause the pipe - sent to us to stop reading data from pipe
	 */
	case object PausePipe extends OutputPipeFlow

	/**
	 * Resume the pipe - sent to us to resume reading data from pipe
	 */
	case object ResumePipe extends OutputPipeFlow


	/**
	 * Message to declare that a read has completed on the pipe
	 *
	 * @param data buffer containing data read
	 * @param eof end-of-file reached
	 */
	case class ReadData(data: ByteBuffer, eof: Boolean)
}
