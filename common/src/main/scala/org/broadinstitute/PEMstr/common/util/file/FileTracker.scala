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

import java.io.Closeable
import org.broadinstitute.PEMstr.common.util.{ValueTracker, StartTracker}

/**
 * @author Nathaniel Novod
 * Date: 1/17/13
 * Time: 11:06 AM
 *
 * Base trait to track opening and closing of a file.
 */
trait FileTracker {
	private val openPending = new StartTracker
	private val openDone = new StartTracker
	private val closeDone = new StartTracker
	private val file = new ValueTracker[Closeable]

	/**
	 * Open to be defined by subclass.
	 * @param spec file specification
	 * @return open file that can be closed later
	 */
	protected def openFile(spec: String) : Closeable

	/**
	 * Open a file - only done previous open not started.
	 * @param spec file specification
	 */
	def open(spec: String) {
		if (!openPending.isSet && !openDone.isSet) {
			openPending.set()
			file() = openFile(spec)
			openDone.set()
		}
	}

	/**
	 * Close a file - only done if close not previously done and open has completed
	 */
	def close() {
		if (openDone.isSet && !closeDone.isSet) {
			file().close()
			closeDone.set()
		}
	}

	/**
	 * Is the file open?
	 * @return true if file is open
	 */
	def isOpen = openDone.isSet && !closeDone.isSet

	/**
	 * Is file open pending?
	 * @return true if open is pending (open started but not completed yet)
	 */
	def isOpenPending = openPending.isSet && !openDone.isSet
}
