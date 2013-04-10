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

package org.broadinstitute.PEMstr.localScheduler.step.socket

import akka.actor.{ActorRef,Status,ActorLogging,Actor}
import java.nio.channels.{SocketChannel, ServerSocketChannel}
import java.nio.ByteBuffer
import scala.concurrent.Future
import util.{Failure, Success}
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.{ByteBufferBufferingQueue,PendingDataQueue,DataQueueLinkedConsumer}
import org.broadinstitute.PEMstr.common.workflow.RewindPipe
import org.broadinstitute.PEMstr.common.util.{ReportBuffers,ValueTracker,StartTracker}
import org.broadinstitute.PEMstr.localScheduler.step.socket.StepInputSocket.
{SocketListening,FirstSocketData}
import org.broadinstitute.PEMstr.localScheduler.step.StepManager.{ResumeSource,PauseSource}
import org.broadinstitute.PEMstr.localScheduler.step.socket.SocketDataEventBus.SocketData
import org.broadinstitute.PEMstr.localScheduler.step.pipe.StepInputPipe
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler.futureExecutor
import annotation.tailrec
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.BufferSettings

/**
 * @author Nathaniel Novod
 * Date: 1/17/13
 * Time: 9:43 PM
 *
 * Manage a socket used for gathering input for a step.
 *
 * @param flow name of data flow
 * @param serverChannel socket server setup to start receiving connections
 * @param stepManager actor managing step
 * @param bus publishes transition states such as when the first data arrives or we've started listening for connections
 * @param pipeSpec file specification for the pipe
 * @param buffering settings for buffering to do for stream
 */
sealed class StepInputSocket(flow: String, serverChannel: ServerSocketChannel, stepManager: ActorRef,
                             bus: SocketDataEventBus, pipeSpec: String, buffering: BufferSettings)
	extends Actor with ActorLogging with PendingDataQueue with ByteBufferBufferingQueue {

	/**
	 * Reporter for multi buffering usage
 	 */
	private val bufReport = new ReportBuffers

	/**
	 * Set level for multi buffering
 	 */
	final override protected val bufferingLevel = buffering.getMultiLvl

	/**
	 * Get step input instance
	 */
	private val inputFile = new StepInputPipe(pipeSpec, log)

	/**
	 * Get the input file - override of this occurs if multiple pipe instances are used for rewind
	 * @return pipe being used to send input to step
	 */
	protected def getInputFile = inputFile

	/*
	 * Data queue used to track data in use by us
	 */
	private val dataQueue = new DataQueueLinkedConsumer(self, this.context.system, buffering) {
		/**
		 * Called when data is published - we simply add the data to the DataHoldingQueue
		 *
		 * @param data transition data being processed
		 */
		override protected def dataProcessor(data: DataQueueLinkedConsumer#Consumer#TransitionData) {
			addPendingData(data)
		}
	}

	/**
	 * Flag to indicate when the pipe is busy.  We are doing blocking requests to the pipe but doing the requests in
	 * separate threads that send back messages to this actor when the request completes.  This flag is set to true
	 * when there is an outstanding request for which we are waiting the completion message.  When inputFileBusy
	 * is set to true additional requests are held up until the completion message from the outstanding request
	 * comes through.
 	 */
	private var inputFileBusy = false

	/**
	 * Flag set to true when we've sent out a paused message to the source.
 	 */
	private var inputPaused = false

	/**
	 * Pause the input source if it's not already paused
	 */
	protected def pauseInput() {
		if (!inputPaused) {
			stepManager ! PauseSource(flow)
			inputPaused = true
		}
	}

	/**
	 * Resume sending of data from the source
	 */
	protected def resumeInput() {
		if (inputPaused) {
			stepManager ! ResumeSource(flow)
			inputPaused = false
		}
	}

	/**
	 * Set after first time we receive data
	 */
	private val firstTime = new StartTracker

	/**
	 * Used to communicate to client that is source of data
	 */
	private val socketChannel = new ValueTracker[SocketChannel]

	/**
	 * Executed before the actor starts up - we start listening for a socket connection.  The server socket channel
	 * is used to establish the connection and then closed down since there should be only one connection made to
	 * the server.
 	 */
	override def preStart() {
		Future {
			bus.publish(SocketListening(serverChannel.socket().getLocalPort))
			serverChannel.accept()
		} onComplete {
			case Success(client) => {
				serverChannel.socket().close()
				self ! Connection(client)
			}
			case Failure(err) => self ! Status.Failure(err)
		}
	}

	/**
	 * All done - close down the socket
 	 */
	override def postStop() {
		if (socketChannel.isSet) socketChannel().close()
	}

	/**
	 * Open the pipe used for feeding input into the step process.
	 *
 	 * @param file pipe file object
	 * @param clientChannel channel getting input from source
	 */
	private def openPipe(file: StepInputPipe, clientChannel: SocketChannel) {
		pauseInput()  // Make sure too much data doesn't arrive before the file is even opened
		inputFileBusy = true
		Future {
			file.connectPipe()
		} onComplete {
			case Success(bytesRead) => {
				self ! FileOpen(clientChannel)
			}
			case Failure(err) => {
				log.error("Error opening pipe: " + err.getMessage)
				context.stop(self)
			}
		}
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
	 * Go read incoming data from the source.
	 *
 	 * @param client socket used to read data
	 */
	private def readData(client: SocketChannel) {

		/**
		 * Use recursion to read from socket till buffer is full to wanted level
		 *
		 * @param socket channel to read from
		 * @param data buffer to read into
		 *
		 * @return (buffer, eof)
		 */
		@tailrec
		def readFullBuf(socket: SocketChannel, data: ByteBuffer): (ByteBuffer, Boolean) = {
			val read = socket.read(data)
			if (read == -1 || buffering.isFilled(data)) (data, read == -1) else readFullBuf(socket, data)
		}

		fillBufferQueue(dataQueue)
		/**
		 * Use a future to read the data but don't mess with the data queue outside actor thread.
		 */
		Future {
			useQueue((buf, bufsLeft) => {
				val (data, eof) = readFullBuf(client, buf)
				val bufsToGo = bufsLeft
				if (bufsToGo > 0) {
					self ! DataRead(client, data, eof)
				} else {
					self ! DataReadNeedBuffer(client, data, eof)
				}
				!eof && bufsToGo != 0
			})
		} onComplete {
			case Success(completionData) =>
			case Failure(err) => {
				log.error("Read failed with: " + err.getMessage)
				self ! Status.Failure(err)
			}

		}
	}

	/**
	 * If the file isn't already busy send out new data.  If we are busy (file being opened or we're writing data)
	 * and there's too much data pending (received over the socket but not written) we send back a request to the
	 * source of the data to pause sending input.
 	 */
	private def sendOutData() {
		if (!inputFileBusy && isDataPending) {
			resumeInput() // Make sure we're not paused
			inputFileBusy = sendData()
		} else if (inputFileBusy && pendingLength >= buffering.getPauseSize) {
			pauseInput()
		}
	}

	/**
	 * Write out data to the pipe.
	 *
 	 * @return true if write is started (always true here but method can be overriden)
	 */
	protected def sendData() = {

		 /* Get data off of pending queue (data that came in from the source that we haven't sent out yet) */
		val outputDataSize = pendingDataSize
		val eof = isEndOfPendingDataIncluded
		val dataToSend = getPendingData

		 /* Go write the data out to the pipe. */
		Future {
			/**
			 * Write out the data - looping till we're sure it's all been written out
			 *
			 * @param data array of data to write out
			 * @param amountToSend total # of bytes that should be sent
			 */
			@tailrec
			def writeData(data: Array[ByteBuffer], amountToSend: Long) {
				if (amountToSend > 0L) {
					val written = getInputFile.write(data)
					writeData(data, amountToSend - written)
				}
			}

			writeData(dataToSend, outputDataSize)
			outputDataSize
		} onComplete {
			case Success(length) => self ! DataWritten(dataToSend, length, eof)
			case Failure(err) => {
				log.error("Write failed with: " + err.getMessage)
				self ! DataWritten(dataToSend, 0, eof = true)
			}
		}
		true
	}


	def processData(client: SocketChannel, buf: ByteBuffer, eof: Boolean) {
		val bytesRead = buf.position()

		/* If the first data read then we publish this and go ready the pipe to write out the data */
		if (!firstTime.isSet) {
			firstTime.set()
			bus.publish(FirstSocketData(bytesRead))
			openPipe(getInputFile, client)
		}

		/* Put the data on the transition queue */
		if (bytesRead > 0) {
			dataQueue.publishData(buf, eof)
		} else {
			dataQueue.freeUnusedBuffer(buf)
			if (eof) {
				dataQueue.publishEndOfData()
			}
		}

		/* Send data out on pipe */
		sendOutData()

		/* If end-of-data shut down the socket */
		if (eof) {
			log.info(bufReport.getCurrentReport(getReportIntro("a total of ")))
			log.info("Closing client connection")
			client.close()
		}
	}

	/**
	 * Go finish up the rewind - here to be overriden for rewind sockets
 	 */
	protected def finishRewind() { }

	/**
	 * Finish up the write - tell the queues the data can now be set free and try to send out more data (unless we've
	 * reached the end-of-data)
	 *
 	 * @param bufs buffers written out
	 * @param bytesWritten # of bytes written
	 * @param eof true if end-of-data written
	 */
	private def finishWrite(bufs: Array[ByteBuffer], bytesWritten: Long, eof: Boolean) {

		/* Tell pending queue we're done with these buffers */
		pendingDataDone(bufs.length)

		/* If not end-of-data then try to send out more, otherwise shut things down */
		if (!eof) {
			sendOutData()
		} else {
			getInputFile.closePipe()
			context.stop(self)
		}
	}

	/**
	 * Message sent when the connection to the client is established.
	 *
 	 * @param client client sending data out to us
	 */
	protected case class Connection(client: SocketChannel)

	/**
	 * Message sent when a read on the input socket has completed
	 *
	 * @param client socket data read from
	 * @param buf contains data read
	 * @param eof true if this is end-of-data
	 */
	protected case class DataRead(client: SocketChannel, buf: ByteBuffer, eof: Boolean)

	/**
	 * Message sent when a read on the input socket has completed and more buffers are needed to read more
	 *
	 * @param client socket data read from
	 * @param buf contains data read
	 * @param eof true if this is end-of-data
	 */
	protected case class DataReadNeedBuffer(client: SocketChannel, buf: ByteBuffer, eof: Boolean)

	/**
	 * Message sent when file (pipe) open completed
	 *
	 * @param client socket to receive data to be written to file
	 */
	protected case class FileOpen(client: SocketChannel)

	/**
	 * Message sent when write of data to pipe has completed
	 *
	 * @param bufs buffers containing data written (position, etc. has been updated)
	 * @param bytesWritten # of bytes written to pipe
	 * @param eof true if end-of-file written
	 */
	protected case class DataWritten(bufs: Array[ByteBuffer], bytesWritten: Long, eof: Boolean)

	/**
	 * Message sent when write of rewind data complete
	 */
	case object RewindDataWritten

	/**
	 * Heart of actor - partial function implementation that receives and processes messages
	 */
	def receive = {
		/*
		 * Connection to client established - we remember the socket and go look for some data
		 *
		 * client: SocketChannel used for connection
		 */
		case Connection(client) => {
			log.info("Client connected, opening output stream for " + pipeSpec)
			socketChannel() = client
			readData(client)
		}

		/*
		 * Data read from client
		 *
		 * client: SocketChannel used for connection
		 * buf: ByteBuffer with data read
		 * eof: true if end-of-data read
		 */
		case DataRead(client, buf, eof) => {
			doReport(0)
			if (eof) emptyBufferQueue(dataQueue) else fillBufferQueue(dataQueue)
			processData(client, buf, eof)
		}

		/*
		 * Data read from client and reading stopped because more buffers are needed.
		 *
		 * client: SocketChannel used for connection
		 * buf: ByteBuffer with data read
		 * eof: true if end-of-data read
		 */
		case DataReadNeedBuffer(client, buf, eof) => {
			doReport(0)
			processData(client, buf, eof)

			/* If not the end-of-data go get more */
			if (!eof) readData(client)
		}

		/*
		 * File open complete - ask for source data to start up again and go send out any pending data
		 *
		 * client: Socket channel
		 */
		case FileOpen(client) => {
			resumeInput()
			inputFileBusy = false
			sendOutData()
		}

		/*
		 * Write of data to file completed
		 *
		 * bufs: ByteBuffers used for write
		 * length: # of bytes written
		 * eof: true if end-of-data sent
		 */
		case DataWritten(bufs, length, eof) => {
			inputFileBusy = false
			finishWrite(bufs, length, eof)
		}

		/*
		 * Rewind data written - now we can restart sending data from the start - note we should only get this
		 * once for the life of the socket and only if this is a "rewind socket".
 		 */
		case RewindDataWritten => {
			assert(this.isInstanceOf[StepInputSocketRewind], "Rewind data written outside rewind socket")
			finishRewind()
			inputFileBusy = false
			sendOutData()
		}
	}

}

/**
 * Special extension of input socket that reads and sends to the pipe some data (specified in the "rewindSpec") and
 * then goes back to the start of the data and retransmits the "rewind" data and whatever more data comes our way.
 * Hopefully this is not a frequent occurrence but it is required for some programs (e.g., tophat).
 *
 * @param flow name of data flow
 * @param serverChannel socket server setup to start receiving connections
 * @param stepManager actor managing step
 * @param bus publishes transition states such as when the first data arrives or we've started listening for connections
 * @param rewindSpec specification of what data should be "rewound"
 * @param pipeSpec file specification for the pipe
 * @param buffering settings for buffering to do for stream
 */
class StepInputSocketRewind(flow: String, serverChannel: ServerSocketChannel, stepManager: ActorRef,
                            bus: SocketDataEventBus, rewindSpec: RewindPipe, pipeSpec: String, buffering: BufferSettings)
	extends StepInputSocket(flow, serverChannel, stepManager, bus, pipeSpec, buffering) {

	/**
	 * Stream used if and only if a rewind takes place and a reopen is requested.  If there is a rewind we initially
	 * write out the data using the outputFile stream and then when it's time to rewrite the start of the data, if a
	 * reopen is requested, we close the initial outputFile and reopen the file using the rewindFile stream below.
	 * A reopen is requested if an end-of-file needs to be sent out between the initial data sent out and the
	 * resending (a.k.a. rewind) of that initial data sent out when the entire stream is output.
 	 */
	private val rewindFile = if (rewindSpec.reopenFile) Some(new StepInputPipe(pipeSpec, log)) else None

	/**
	 *  Flag to indicate that the rewind has been completed  - we wrote out the initial data and restarted
	 */
	private val rewindDone = new StartTracker

	/**
	 * Get which input file is being used.  If we're finished rewinding and had to reopen the file then we return
	 * the reopened file.  Otherwise we use the file originally open.
	 *
	 * @return pipe being used to send input to step
	 */
	override protected def getInputFile =
		if (rewindDone.isSet && rewindFile.isDefined) rewindFile.get else super.getInputFile

	/**
	 * If our rewind is complete then we just send out data as usual.  Otherwise we need to check if the source data
	 * received so far contains what is wanted before we commence to rewind.  If the source data matches what is wanted
	 * we write it out, otherwise we wait for more data from the source.
	 *
 	 * @return true if write is done, false if we need to get more data before writing out data to be rewound
	 */
	override protected def sendData() = {
		if (rewindDone.isSet) super.sendData() else {
			/* See if we have all the data we need for first write */
			rewindSpec.getRewindLen(pendingDataAsByteBufferArray) match {
				case Some(len) => {
					/* We've got what we need - write it out and send back a message when all done */
					val dataToSend = slicePendingData(len)
					Future {
						var totalWritten = 0L
						while (totalWritten < len) {
							totalWritten += getInputFile.write(dataToSend)
						}
					} onComplete {
						case Success(length) => self ! RewindDataWritten
						case Failure(err) => {
							log.error("Write failed with: " + err.getMessage)
							self ! RewindDataWritten
						}
					}
					true
				}
				/* Still need more data before writing out initial data to be rewound */
				case _ => false
			}
		}
	}

	/**
	 * The data to be rewound has been sent out.  Now we need to set things up so we can go back to the start and
	 * rewrite what was sent out so far.
 	 */
	override protected def finishRewind() {
		log.info("Initiating rewind of data")

		/* If we're going to reopen the file go close it first */
		if (rewindSpec.reopenFile) {
			/*
			 *  Pause data coming in and then close the pipe being input to the process.  We tell the source to
			 *  start sending data once a reopen of the pipe takes place.  It is crucial that the source
			 *  pause sending data so that we do not get overwhelmed (e.g, run out of memory) if the
			 *  program doing the rewind takes a long break between reading the data at the start of the
			 *  file and when it "rewinds" to restart reading from the file.  This was occurring if
			 *  multiple files were given to tophat because the reopen of the second file would not take place
			 *  until tophat had read the entire first file.  tophat first reads just a little bit from all the
			 *  files to check that the files' format is correct.  It then closes all the files and hands them off
			 *  to the main alignment code which reads in entire files, one at a time.
			 */
			pauseInput()
			getInputFile.closePipe()
		}

		/* Do optional delay before resuming activity - used to allow process time to close and reopen as well */
		if (rewindSpec.delay != 0) {
			log.info("Start rewind delay")
			Thread.sleep(rewindSpec.delay * 1000)
			log.info("End rewind delay")
		}

		/* If reopening, now's the time to reopen the file and resume getting data */
		if (rewindSpec.reopenFile) {
			/*
			 *  Reopen the file to send new data (this blocks till other side has pipe open) and then tell
			 *  the source to start sending data once the reopen completes.
			 */
			log.info("Reopening pipe")
			rewindFile.get.connectPipe()
			log.info("Pipe reopen")
			resumeInput()
		}

		/* Flag that all activity from now on can be "normal" - the rewind back to the start has completed */
		rewindDone.set()
	}
}

/**
 * Actor messages sent for socket
 */
object StepInputSocket {
	/**
	 * Message sent to subscribers when the socket has started listening for new connections
	 * @param port port # we're listening on
	 */
	case class SocketListening(port: Int) extends SocketData

	/**
	 * Message sent to subscribers when the socket receives its first data
	 * @param size # of bytes received
	 */
	case class FirstSocketData(size: Int) extends SocketData
}
