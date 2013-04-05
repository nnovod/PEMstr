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

import java.io.FileInputStream
import annotation.tailrec
import java.nio.ByteBuffer
import org.broadinstitute.PEMstr.common.util.ValueTracker

/**
 * @author Nathaniel Novod
 * Date: 1/17/13
 * Time: 11:48 AM
 *
 * Maintains an input file stream including open, close and read operations.
 */
class FileInputStreamTracker extends FileTracker {
	/**
	 * Keep track of the stream we're using
	 */
	private val stream = new ValueTracker[FileInputStream]

	/**
	 * Call back from FileTracker to open stream.
	 *
 	 * @param file file specification
	 *
	 * @return open stream
	 */
	protected def openFile(file: String) = {
		stream() = new FileInputStream(file)
		stream()
	}

	/**
	 * Go read data into buffer
	 *
 	 * @return data read and eof flat
	 */
	def read(buf: ByteBuffer) = {
		assert(isOpen)
		/* This will block until some data is available or eof */
		val dataRead = stream().getChannel.read(buf)
		(buf, dataRead == -1)
	}
}
