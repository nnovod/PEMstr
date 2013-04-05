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

package org.broadinstitute.PEMstr.localScheduler.step

import akka.actor.{Status,ActorLogging,Actor}
import org.broadinstitute.PEMstr.common.util.file.FileOutputStreamTracker
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.DataQueue.DataToProcess
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.PendingDataQueue
import concurrent.Future
import util.{Failure,Success}
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler.futureExecutor

/**
 * @author Nathaniel Novod
 * Date: 3/7/13
 * Time: 1:49 PM
 *
 * Manages the checkpoint file used in conjunction with output being written from a step.  We are setup as a consumer
 * of an output pipe being read from the step.
 *
 * @param fileSpec file specification for checkpoint file
 */
class StepCheckpointFile(fileSpec: String)
	extends Actor with ActorLogging with PendingDataQueue {
	/**
	 * Checkpoint file tracker
	 */
	private val checkpointFile = new FileOutputStreamTracker

	/**
	 * Message sent to ourselves when file open completes.
	 *
 	 * @param file tracker for open output stream
	 */
	private case class FileOpen(file: FileOutputStreamTracker)

	/**
	 * Called before actor starts - we open the checkpoint file
 	 */
	override def preStart() {
		log.info("Opening checkpoint file: " + fileSpec)
		Future {
			checkpointFile.open(fileSpec)
			checkpointFile
		} onComplete {
			case Success(file) => self ! FileOpen(file)
			case Failure(err) => self ! Status.Failure(err)
		}
	}

	/**
	 * Called after actor is terminated - we close the checkpoint file
	 */
	override def postStop() {
		log.info("Closing checkpoint file")
		checkpointFile.close()
	}

	/**
	 * Send out any data collected so far if checkpoint file is open.
 	 */
	private def sendOutData() {
		if (checkpointFile.isOpen) {
			val dataToWrite = pendingDataSize
			val eod = isEndOfPendingDataIncluded
			if (dataToWrite != 0) {
				var dataWritten = 0L
				while (dataWritten < dataToWrite) {
					val writeLength = checkpointFile.write(pendingDataAsByteBufferArray)
					dataWritten += writeLength
				}
			}
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
		/**
		 * The file is open - write out any data that has arrived since we started
		 */
		case FileOpen(file) => sendOutData()
		/**
		 *
		 * There's more data to process - write it out to the checkpoint file.
		 *
		 * data: Additional data from producer to be consumed
 		 */
		case DataToProcess(data) => {
			addPendingData(data)
			sendOutData()
		}
	}
}
