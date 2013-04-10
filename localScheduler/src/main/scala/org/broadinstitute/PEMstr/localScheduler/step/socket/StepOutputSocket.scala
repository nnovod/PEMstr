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

import akka.actor.{ActorLogging,Actor,Status}
import java.nio.channels.SocketChannel
import java.net.{Socket,InetSocketAddress}
import scala.concurrent.Future
import org.broadinstitute.PEMstr.common.util.{ValueTracker, ReportBytes}
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.{DataQueue,PendingDataQueue}
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.DataQueue.DataToProcess
import util.{Failure, Success}
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler.futureExecutor
import annotation.tailrec
import java.nio.ByteBuffer


/**
 * @author Nathaniel Novod
 * Date: 1/15/13
 * Time: 5:26 PM
 *
 * Manages socket used to output data from a step.
 *
 * @param host name of server socket host node
 * @param port server socket port #
 */
class StepOutputSocket(host: String,  port: Int)
  extends Actor with ActorLogging with PendingDataQueue {

	/**
	 * Reporter for # of bytes used
 	 */
	private val reportBytes = new ReportBytes

	/* channel used to connect to server */
	private val clientChannel = SocketChannel.open()

	/* Place to remember socket handle */
	private val outputSocket = new ValueTracker[Socket]

	/* Message sent when connection is established */
	private case class Connection(socket: Socket)

	/**
	 * Called before the actor starts up.  We connect to the socket.
	 */
	override def preStart() {
		log.info("preStart")
		log.info("Connecting to job output socket on host: " + host + " port: " + port)
		Future {
			clientChannel.connect(new InetSocketAddress(host, port))
			clientChannel.socket() // Return socket channel for completion
		} onComplete {
			case Success(socket) => {
				self ! Connection(socket)
			}
			case Failure(err) => self ! Status.Failure(err)
		}
	}

	/**
	 * Called after actor has been terminated.  We issue a report on the socket activity and close the socket and
	 * associated channel.
	 */
	override def postStop() {
		log.info("postStop")
		log.info(reportBytes.getCurrentReport("Socket wrote total of "))
		if (outputSocket.isSet && outputSocket().isConnected) {
			log.info("Closing socket")
			outputSocket().close()
		}
		if (clientChannel.isOpen) {
			log.info("Closing socket channel")
			clientChannel.close()
		}

	}

	/**
	 * Send available data over the socket.
	 *
	 * @param data contains data built up to be send over socket
	 */
	private def sendData(data: DataQueue#Consumer#TransitionData) {
		addPendingData(data)
		sendOutData()
	}

	/**
	 * Send out any data collected so far if socket is connected.
 	 */
	private def sendOutData() {
		if (outputSocket.isSet && outputSocket().isConnected) {
			val dataToWrite = pendingDataSize
			val eod = isEndOfPendingDataIncluded

			/**
			 * Write out the data - looping till we're sure it's all been written out
			 *
			 * @param data array of data to write out
			 * @param amountToSend total # of bytes that should be sent
			 */
			//@TODO Use selector of OP_WRITE to see when writable to avoid loop when socket buffer full?
			@tailrec
			def writeData(data: Array[ByteBuffer], amountToSend: Long) {
				if (amountToSend > 0L) {
					val written = outputSocket().getChannel.write(data)
					writeData(data, amountToSend - written)
				}
			}

			writeData(pendingDataAsByteBufferArray, dataToWrite)
			val nextReport = reportBytes.dataReport("Socket wrote ", dataToWrite)
			if (nextReport.isDefined) log.info(nextReport.get)
			clearPendingData()
			if (eod) {
				log.info("Received end-of-data")
				context.stop(self)
			}
		}
	}

	/**
	 * Heart of the actor - receives messages
	 *
 	 * @return
	 */
	def receive = {
		/*
		 * Connection made to the socket - we save socket and send out any data built up so far.
		 *
		 * socket: Client Socket connection was made on
		 */
		case Connection(socket) => {
			log.info("Connected to server address " + socket.getRemoteSocketAddress)
			outputSocket() = socket
			sendOutData() // Send out any data pending
		}

		/**
		 *
		 * There's more data to process - send it out over the socket.
		 *
		 * data: Additional data on queue to process
 		 */
		case DataToProcess(data) => sendData(data)
	}

}
