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

package org.broadinstitute.PEMstr.localScheduler.util.dataQueue

import scala.collection.mutable
import scala.annotation.tailrec
import java.nio.ByteBuffer

/**
 * @author Nathaniel Novod
 *         Date: 4/9/13
 *         Time: 2:08 PM
 *
 * Queue used to keep available buffers for usage.  The queue itself is synchronized but the trait is not.
 * This means that it is safe for one thread to fill the queue while another one uses the queue.  However it is not
 * safe for multiple threads to fill the queue at the same time nor to use the queue at the same time.
 *
 * Note that this trait simply manages the queue.  Exactly what the queue is filled with, how the queue entries are
 * used and how the entries are freed is determined by callbacks supplied to this trait's methods.
 */
trait BufferingQueue[T] {
	/**
	 * The synchronized queue
	 */
	private val queue = new mutable.SynchronizedQueue[T]

	/**
	 * Default size of queue - can use override to change size
	 */
	protected val bufferingLevel = 2

	/**
	 * Fill the queue up to its size
	 *
	 * @param getData given a number of buffers wanted, returns an array of buffers
	 */
	final def fillQueue(getData: (Int) => Array[T]) {
		val fillSize = bufferingLevel - queue.size
		if (fillSize > 0) {
			val bufs = getData(fillSize)
			/* Put buffers returned onto queue (note array is transformed into repeated parameters) */
			queue.enqueue(bufs: _*)
		}
	}

	/**
	 * Use the buffers in a queue.  Each buffer is removed and a callback is given the buffer to use.  The callback
	 * can see how many buffers are left (note this is by name so it's up to callback when it wants to check if
	 * queue is empty) before returning if it wants to be called again to process another buffer.
	 *
 	 * @param useData called with buffer to use and by name value to get size of queue; returns if another buffer wanted
	 */
	@tailrec
	final def useQueue(useData: (T, =>Int) => Boolean) {
		val continue = queue.dequeueFirst((s) => true) match {
			case Some(d) => {
				useData((d), queue.size)
			}
			case None => false
		}
		if (continue) useQueue(useData)
	}

	/**
	 * Empty the queue
	 *
 	 * @param freeData called with buffer removed from queue
	 */
	final def emptyQueue(freeData: (T) => Unit) {
		queue.dequeueAll((d) => {
			freeData(d)
			true
		})
	}
}

trait ByteBufferBufferingQueue extends BufferingQueue[ByteBuffer] {
	/**
	 * Free all the buffers on the buffer queue used for reading
	 */
	def emptyBufferQueue(dataQueue: DataQueueCore) {
		emptyQueue((buf) => dataQueue.freeUnusedBuffer(buf))
	}


	/**
	 * Fill the buffer queue used for reading
 	 */
	def fillBufferQueue(dataQueue: DataQueueCore) {
		fillQueue((numBufs) => {
			(for (x <- 1 to numBufs) yield dataQueue.getBuffer).toArray
		})
	}
}
