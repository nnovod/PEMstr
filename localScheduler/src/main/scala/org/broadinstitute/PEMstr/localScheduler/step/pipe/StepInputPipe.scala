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

import akka.event.LoggingAdapter
import org.broadinstitute.PEMstr.common.util.ReportBytes
import org.broadinstitute.PEMstr.common.util.file.FileOutputStreamTracker
import java.nio.ByteBuffer

/**
 * @author Nathaniel Novod
 * Date: 1/24/13
 * Time: 4:54 PM
 *
 * Manages the pipe used as input to a step.
 *
 * @param pipeSpec file specification for pipe
 * @param logger where to log messages
 */
class StepInputPipe(pipeSpec: String, logger: LoggingAdapter) extends ReportBytes {
	private val outputFile = new FileOutputStreamTracker

	/**
	 * Connect to the pipe - this will block until the pipe is open so it's best to call it in a separate thread
	 */
	def connectPipe() {
		if (!outputFile.isOpen) {
			logger.info("Opening pipe: " + pipeSpec)
			outputFile.open(pipeSpec)
			logger.info("Pipe opened")
		}
	}

	/**
	 * Write out data to be read as input by process.  The write blocks till complete.
	 *
	 * @param data data to be written out
	 *
	 * @return # of bytes written
	 */
	def write(data: Array[ByteBuffer]) = {
		val outputSize = outputFile.write(data)
		val nextReport = dataReport("Pipe wrote out ", outputSize, "write")
		if (nextReport.isDefined) logger.info(nextReport.get)
		outputSize
	}

	/**
	 * Shut down the pipe.  We report on the pipes activity and then do a close on the file.
	 */
	def closePipe() {
		logger.info("Closing pipe")
		logger.info(getCurrentReport("Pipe wrote out total of ", "write"))
		outputFile.close()
	}

}
