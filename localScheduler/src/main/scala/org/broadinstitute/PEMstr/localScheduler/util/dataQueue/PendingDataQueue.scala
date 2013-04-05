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

import collection.mutable
import java.nio.ByteBuffer
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.DataQueue.isEndOfDataBuffer

/**
 * @author Nathaniel Novod
 * Date: 1/20/13
 * Time: 8:59 PM
 */
/**
 * Trait used by consumers to keep track of data that has been "published" for use by a producer.  Typically we don't
 * use this data immediately (often we need to wait for other activity, such as previous writes of data, to complete).
 * Between the time when the data is published and when we are use the data it sits in a PendingDataQueue.
 */
trait PendingDataQueue {
	/**
	 * Queue to keep data being held - actually a ListBuffer to allow bulk removes.
	 */
	private val pending = mutable.ListBuffer.empty[ByteBuffer]

	/**
	 * Queue of counters used for buffers - there should only be one counter used for the entire trait instantiation
	 * but to keep things totally proper we use this queue with hopefully very low overhead for a single entry.
 	 */
	private val counter = new mutable.Queue[(Counter, Int)]

	/**
	 * Keep track of new entry a counter is responsible for.  Note that it is assumed that entries are used in a FIFO
	 * manner.
	 *
	 * @param currentCounter counter used for new entry
	 */
	private def countEntry(currentCounter: Counter) {
		if (counter.isEmpty || !counter.head._1.eq(currentCounter)) {
			counter.enqueue((currentCounter, 1))
		} else {
			val (headCounter, counterCount) = counter.head
			counter(0) = (headCounter, counterCount + 1)
		}
	}

	/**
	 * Declare we're done with some number of items on the pending queue.  It is assumed that items are used in a FIFO
	 * manner.  This is our one link back to the main DataQueue when we tell the "counter" that we are all done with
	 * the buffers so they can be freed.
	 *
 	 * @param done # of items done
	 */
	def pendingDataDone(done: Int) {
		var toGo = done
		while (toGo > 0) {
			/* Better be a counter there.  Otherwise we're trying to count something as done never set as pending */
			val (nextCounter, counterCount) = counter.head
			/*
			 * We do the dequeue here (rather than after the counter is reset) so that we don't wind up constantly
			 * dequeueing and then requeueing the counter when there's only one counter used.  In theory multiple
			 * counters can be used but in reality only one counter is ever used per consumer (so only one is used for
			 * each PendingDataQueue) so counterCount <= 0 should always be false.
			 */
			if (counterCount <= 0) counter.dequeue() else {
				val fromCounter = math.min(toGo, counterCount)
				nextCounter.incCounter(fromCounter)
				toGo -= fromCounter
				counter(0) = (nextCounter, counterCount - fromCounter)
			}
		}
	}

	/**
	 * Get # of entries in pending queue
	 */
	def pendingLength = pending.length

	/**
	 * Is there any pending data?
 	 */
	def isDataPending = !pending.isEmpty

	/**
	 * Add data to the end of the pending queue.
	 *
	 * @param data transition data
	 */
	def addPendingData(data: DataQueueCore#ConsumerCore#TransitionDataCore) {
		countEntry(data.getCounter)
		pending += data.getData
	}

	/**
	 * Convert array of pending data into an array of ByteBuffers.
	 *
	 * @return ByteBuffer array
	 */
	def pendingDataAsByteBufferArray = pending.toArray

	/**
	 * Clear data - we tell the DataQueue that we're done with the transition data and clear out the pending queue.
	 */
	def clearPendingData() {
		pendingDataDone(pending.size)
		pending.clear()
	}

	/**
	 * Get size of all the pending data
	 *
 	 * @return # of bytes in pending data
	 */
	def pendingDataSize = pending.foldLeft(0L)(_ + _.limit())

	/**
	 * Is it the end-of-data?
	 *
	 * @return true if end-of-data marker found at end of pending data
	 */
	def isEndOfPendingDataIncluded = pending.length >= 1 && isEndOfDataBuffer(pending.last)

	/**
	 * Creates a new array of ByteBuffers with the start of the saved data up to a specified number of bytes.
	 * If there are not enough bytes to satisfy the request then all the data available is returned.
	 *
 	 * @param size # of bytes wanted
	 *
	 * @return array of ByteBuffers up to the requested size
	 */
	def slicePendingData(size: Int) = {

		/**
		 * Create a duplicate for a ByteBuffer that is only a piece of the original one.  The copy is limited
		 * in size by setting the new ByteBuffer's limit.
		 *
		 * @param original ByteBuffer to duplicate
		 * @param size # of bytes of original to have duplicate contain
		 *
		 * @return duplicate ByteBuffer with specified # of bytes
		 */
		def duplicate(original: ByteBuffer, size: Int) = {
			val newBuf = original.duplicate()
			val newSize = math.min(newBuf.limit(), size)
			newBuf.limit(newSize)
			newBuf
		}

		/*
		 * Go through transition queue creating a new array of ByteBuffers that only contains the data
		 * of the wanted length.
		 */
		val (array, _) =
			pending.foldLeft((Array.empty[ByteBuffer], 0))((soFar, next) => {
				val (arraySoFar, sizeSoFar) = soFar
				val nextSize = math.min(next.limit(), size - sizeSoFar)
				if (nextSize > 0) {
					(arraySoFar :+ duplicate(next, nextSize), sizeSoFar + nextSize)
				} else soFar
			})
		array
	}

	/**
	 * Get whatever data is pending.  Up to caller to later call pendingDataDone with count to clear the data.
	 *
 	 * @return All entries currently on the queue
	 */
	def getPendingData = {
		val data = pending.toArray
		pending.clear()
		data
	}
}
