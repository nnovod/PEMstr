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

package org.broadinstitute.PEMstr.common.util.file

import java.io.FileOutputStream
import java.nio.ByteBuffer
import org.broadinstitute.PEMstr.common.util.ValueTracker

/**
 * @author Nathaniel Novod
 * Date: 1/17/13
 * Time: 11:26 AM
 *
 * Maintains an output file stream including open, close and write operations.
 */
class FileOutputStreamTracker extends FileTracker {
	/**
	 * Keep track of the stream we're using
	 */
	private val stream = new ValueTracker[FileOutputStream]

	/**
	 * Call back from FileTracker to open stream.
	 *
 	 * @param file file specification
	 *
	 * @return open stream
	 */
	protected def openFile(file: String) = {
		stream() = new FileOutputStream(file)
		stream()
	}

	/**
	 * Write out data to the output stream.
	 *
 	 * @param data ByteBuffers to write out to stream
	 *
	 * @return number of bytes written to stream
	 */
	def write(data: Array[ByteBuffer]) = {
		assert(isOpen)
		stream().getChannel.write(data)
	}

	/**
	 * Write out data to the output stream.
	 *
 	 * @param data ByteBuffer to write out to stream
	 *
	 * @return number of bytes written to stream
	 */
	def write(data: ByteBuffer) = {
		assert(isOpen)
		stream().getChannel.write(data)
	}
}
