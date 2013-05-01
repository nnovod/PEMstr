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
import org.broadinstitute.PEMstr.localScheduler.step.pipe.StepOutputPipe.{ReadDataNeedBuffer,ReadData,PausePipe,ResumePipe}
import scala.concurrent.Future
import java.nio.ByteBuffer
import collection.immutable.Map
import org.broadinstitute.PEMstr.common.util.file.FileInputStreamTracker
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.{ByteBufferBufferingQueue,DataQueue}
import util.{Failure, Success}
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler.futureExecutor
import annotation.tailrec
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.{IgnoreEOF,BufferSettings}
import org.broadinstitute.PEMstr.common.util.{StartTracker,ReportBuffers}

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

sealed class StepOutputPipe(consumers: Map[String, ActorRef], output: String,
                     buffering: BufferSettings)
	extends Actor with ActorLogging with ByteBufferBufferingQueue {

	/**
	 * Reporter for multi buffering usage
 	 */
	private val bufReport = new ReportBuffers

	/**
	 * Set level for multi buffering
 	 */
	final override protected val bufferingLevel = buffering.getMultiLvl
	/**
	 * If positive then it's time to pause
 	 */
	private var pause = 0

	/**
	 * Track the output pipe
	 */
	private val inputStream = new FileInputStreamTracker

	/**
	 * Get the input stream (can be overriden to use different input streams)
	 *
	 * @return input stream
	 */
	protected def getInputStream = inputStream

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
			getInputStream.open(output)
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
		getInputStream.close()
	}

	/**
	 * Send ourselves a message to start reading
	 */
	private def startReader() {
		self ! StartReading
	}

	/**
	 * Process new buffer of data that has been read.  We publish that the data has arrived to interested parties.
	 *
 	 * @param data byte data read
	 * @param eof true if end-of-file reached
	 *
	 * @return eof
	 */
	protected def processData(data: ByteBuffer, eof: Boolean) = {
		if (data.position > 0 || eof) {
			if (eof) log.info(bufReport.getCurrentReport(getReportIntro("a total of ")))
			if (data.position > 0) {
				if (debugEnabled)
					log.debug("Publishing " + data.position + " bytes of data for " + output)
				dataQueue.publishData(data, eof)
			} else {
				log.info("Publishing end-of-file for " + output)
				dataQueue.publishEndOfData()
			}
		}
		eof
	}

	/**
	 * Get introduction for report of queue usage: "Buffering for (actir) refilled (intro)"
	 *
 	 * @param intro end of intro string
	 *
	 * @return beginning of report
	 */
	private def getReportIntro(intro: String) = {
		"Buffering for " + self.path.name + " refilled " + intro
	}

	/**
	 * Method to maintain and periodically report on # of times buffers queue had to be refilled
	 *
	 * @param i # of items to
	 */
	private def doReport(i: Int) {
		val report = bufReport.dataReport(getReportIntro(""), i)
		if (report.isDefined) log.info(report.get)
	}

	/**
	 * Method called to read data from the pipe.  When we've read the wanted amount of data we send a message back
	 * to the actor with the data read.
 	 */
	protected def read() {
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
			 * Make sure we have buffers available
			 */
			fillBufferQueue(dataQueue)
			/**
			 * Use a future to read the data but don't mess with the data queue outside actor thread.
			 */
			Future {
				useQueue((buf, bufsLeft) => {
					val (data, eof) = readData(getInputStream, buf)
					val bufsToGo = bufsLeft
					if (bufsToGo > 0) {
						self ! ReadData(data, eof)
					} else {
						self ! ReadDataNeedBuffer(data, eof)
					}
					!eof && bufsToGo != 0
				})
			} onComplete {
				case Success(result) =>
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
		 * Data read - now we publish the data that has been received and refill the buffer queue if it's not the
		 * end-of-file.  Note that the thread reading data keeps reading so long as there as there are entries
		 * in the buffer queue.
		 *
		 * data: DataBuffer containing data read
		 * eof: true if end-of-file reached
		 */
		case ReadData(data, eof) => {
			doReport(0)
			if (!eof && pause <= 0) fillBufferQueue(dataQueue)
			val done = processData(data, eof)
			if (done) emptyBufferQueue(dataQueue)
		}

		/*
		 * Data read - now we publish the data that has been received and start a new thread to read more data if it's
		 * not the end-of-file.
		 *
		 * data: DataBuffer containing data read
		 * eof: true if end-of-file reached
		 */
		case ReadDataNeedBuffer(data, eof) => {
			doReport(1)
			val done = processData(data, eof)
			/* If eof don't do a stop of actor here.  DataQueue will do that once published data is all digested */
			if (done) emptyBufferQueue(dataQueue) else read()
		}

		/*
		 * Unknown message
		 */
		case e => log.warning("received unknown message: " + e)
	}

}

/**
 *
 * Special output pipe that allows multiple output streams.  When we see the first end-of-file we do not shut down the
 * stream but simply ignore it and close and reopen the pipe if requested.  This allows steps to stream out "preface"
 * files to the pipe before the main output arrives.
 *
 * @param consumers map of consumers of data containing (consumer_name -> ActorRef for consumer)
 * @param output Name of output pipe
 * @param buffering settings for buffering to do for stream
 * @param ignoreEOF specification for reopen/delay to use when ignoring EOF
 */
final class StepOutputPipeIgnoreEOF(consumers: Map[String, ActorRef], output: String,
                                    buffering: BufferSettings, ignoreEOF: IgnoreEOF)
	extends StepOutputPipe(consumers, output, buffering) {

	/**
	 *  Flag to indicate that the reopen has been completed  - we processed one eof and closed and reopened the file
	 */
	private val reopenDone = new StartTracker

	/**
	 * Track the reopened output pipe
	 */
	private val reopenInputStream = new FileInputStreamTracker

	/**
	 * Get original input stream if reopen not done, otherwise get reopen input stream.
	 *
 	 * @return stream currently in use
	 */
	override protected def getInputStream =
		if (reopenDone.isSet && ignoreEOF.isReopen) reopenInputStream else super.getInputStream

	/**
	 * Process new buffer of data that has been read.  We publish that the data has arrived to interested parties.  If
	 * an end-of-file is being seen for the first time we publish the data as if there was no eof and if requested
	 * close and reopen the file to receive new data.
	 *
 	 * @param data byte data read
	 * @param eof true if end-of-file reached
	 *
	 * @return eof
	 */
	override protected def processData(data: ByteBuffer, eof: Boolean) = {
		val endNow = eof && reopenDone.isSet
		super.processData(data, endNow)

		/**
		 * If first eof we've seen we close and reopen the file to get more input
		 */
		if (eof && !reopenDone.isSet) {
			log.info("Received eof to be ignored from " + output)
			if (ignoreEOF.isReopen) {
				getInputStream.close()
			}
			if (ignoreEOF.getDelay != 0) {
				log.info("Executing delay after eof received from " + output)
				Thread.sleep(ignoreEOF.getDelay * 1000)
			}
			if (ignoreEOF.isReopen) {
				log.info("Reopening " + output)
				reopenInputStream.open(output)
			}
			reopenDone.set()
			read()
		}
		endNow
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

	/**
	 * Message to declare that a read has completed on the pipe
	 *
	 * @param data buffer containing data read
	 * @param eof end-of-file reached
	 */
	case class ReadDataNeedBuffer(data: ByteBuffer, eof: Boolean)
}
