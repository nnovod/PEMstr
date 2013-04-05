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

import collection.immutable.Map
import java.nio.ByteBuffer
import akka.actor.{ActorSystem, ActorRef}
import collection.mutable
import org.broadinstitute.PEMstr.common.util.ReportBuffers
import org.broadinstitute.PEMstr.localScheduler.util.dataQueue.DataQueue.
{DataToProcess, isEndOfDataBuffer}
import scala.concurrent.duration._
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.BufferSettings

/**
 * @author Nathaniel Novod
 * Date: 1/17/13
 * Time: 1:57 PM
 */

/**
 * Used to save data arriving.  That data is all contained in ByteBuffers, thus avoiding copying of data between
 * the producer and consumers.  For synchronization purposes there are two groups of actors that can mess with the
 * data: consumers and the single producer.  The producer is the only one that can alter the contents of the queues
 * in any way except for one exception noted later.  It is crucial that only the producer alter contents so that
 * synchronization does not become a problem since it is assumed that the producer is a single actor that uses
 * normal actor synchronization to stay single threaded, only processing one message at a time.
 *
 * The producer gets buffers and then declares when the data is ready for publication.  Upon publication the data
 * is sent out to the consumers which then says when it has finished using the data (that's the one exception and for
 * that an akka agent is used unless it is known that the producer and consumer are the same actor (true for step
 * input)).  Once the consumers are done the buffers can be freed up for use again by the producers.
 *
 * One final function of the DataQueueCore is to terminate the producer actor (assuming it is separate from the
 * consumer actor(s)) once the end-of-data has been published and consumed.
 *
 * @param producer data creator
 * @param consumerActors list of actors that will consumer the data
 * @param system active akka system that producer and consumers live under
 * @param bufferUsage size of buffers and max # of free buffers to keep cached
 */
sealed abstract class DataQueueCore(producer: ActorRef, consumerActors: Map[String, ActorRef], system: ActorSystem,
                                    bufferUsage: BufferSettings)
	extends ReportBuffers with SimpleCounter {

	/* @TODO Do something like this to get amount direct IO buffers available?
	          val c = Class.forName("java.nio.Bits")
	          val maxMemory = c.getDeclaredField("maxMemory")
	          maxMemory.setAccessible(true)
	          val reservedMemory = c.getDeclaredField("reservedMemory")
	          reservedMemory.setAccessible(true)
	          synchronized (c) {
	          val maxMemoryValue = maxMemory.get(null)
	          val reservedMemoryValue = reservedMemory.get(null)
	          }
	 */

	private val bufferSize = bufferUsage.getBufferSize
	private val keepFree = bufferUsage.getCacheSize

	/**
	 * Save this since it takes so long to continually get it for the report (actually showed up as 1% overhead - not
	 * that significant but very easy to eliminate)
	 */
	private val producerPath = producer.path.toString

	/**
	 * Announce our arrival
 	 */
	system.log.info("Starting data queue for producer " + producerPath +
	  " and consumer(s) " + consumerActors.keys.mkString(", ") + " with buffer size = " + bufferSize +
	  ", free queue size = " + keepFree + ", fill % = " + bufferUsage.getFillPct +
	  ", pause = " + bufferUsage.getPauseSize)

	/**
	 * Consumers list
 	 */
	private val consumers = {
		/**
		 * Get last consumer name.
		 */
		val lastConsumerName = consumerActors.lastOption match {
			case Some(headConsumer) => {
				val (consumerName, _) = headConsumer
				consumerName
			}
			case None => "None"
		}

		/**
		 * Now make the list marking the last entry
 		 */
		consumerActors.map((next) => {
			val (name, actorRef) = next
			getNewConsumer(name, actorRef, name == lastConsumerName)
		})
	}

	/**
	 * Get last consumer entry
	 */
	private val lastConsumer = {
		val lastEntry = consumers.lastOption
		if (lastEntry.isDefined) assert(lastEntry.get.isLast, "Last entry not last")
		lastEntry
	}

	/**
	 * End-of-data marker
	 */
	private val endOfDataBuffer = ByteBuffer.allocate(0).asReadOnlyBuffer()

	/**
	 * Available byte buffers - we cache some number of them to avoid continually needing to allocate new one.  It
	 * should at least make Java garbage collection need to occur less.
	 */
	private val freeQueue = new mutable.ListBuffer[ByteBuffer]

	/**
	 * Get introduction for report of queue usage: "Data queue for (producer) allocated (intro)"
	 *
 	 * @param intro end of intro string
	 *
	 * @return beginning of report
	 */
	private def getReportIntro(intro: String) = {
		"Data queue for " + producerPath + " allocated " + intro
	}

	/**
	 * Records # of direct buffer allocation failures
	 */
	private var directAllocationFailures = 0

	/**
	 * Get available buffer.  If there are none on the free queue we do garbage collection to free up any buffers
	 * that the consumers are done with.  If there is still no buffer on the free queue after the garbage is
	 * collected we allocate a new one.
 	 */
	def getBuffer : ByteBuffer = {

		/* Method to maintain and periodically report on # of buffers allocated */
		def doReport(i: Int) {
			val report = dataReport(getReportIntro(""), i)
			if (report.isDefined) system.log.info(report.get)
		}

		/* Do a garbage collection if the free queue is empty */
		if (freeQueue.isEmpty) {
			doGC()
		}
		/* Now go get a buffer */
		if (freeQueue.isEmpty) {
			/* Still no buffer available so make a new one and remember that for our report */
			doReport(1)
			try {
				ByteBuffer.allocateDirect(bufferSize)
			} catch {
				/* If we run out of direct buffers then use a "normal" one */
				//@TODO Need to do more?  After we get here then I/O often failing with allocation failure
				// trying to get "temporary" direct buffer
				case mem: OutOfMemoryError => {
					directAllocationFailures += 1
					ByteBuffer.allocate(bufferSize)
				}
				case e: Exception => throw e
			}
		} else {
			val buf = freeQueue.remove(0)
			/* Make sure we're not giving back the EOF buffer - should very rarely happen if ever */
			if (!isEndOfDataBuffer(buf)) {
				/* Report one more operation but no allocation */
				doReport(0)
				buf.clear()
				buf
			} else getBuffer
		}
	}


	/**
	 * Free a buffer gotten but then never published.  This call should be very rare (why get a buffer if you're not
	 * going to use it?) so we make real sure it's not used anywhere it shouldn't be by checking that no consumers
	 * are referencing the buffer.
	 *
 	 * @param buf allocated via getBuffer
	 */
	def freeUnusedBuffer(buf: ByteBuffer)  {
		if (freeQueue.size < keepFree && buf.isDirect) freeQueue += buf
	}

	/**
	 * Do garbage collection.  Buffers that all consumers are done with are freed.  Garbage collection is kept
	 * simple (so it doesn't take up much time) but effective (it does free unused buffers).  Since any buffer
	 * published is sent to all consumers we simply maintain one counter associated with the entire data queue that
	 * marks what was the number of the last buffer that was freed during garbage collection and one counter in each
	 * consumer indicating the last buffer that the consumer finished using.  When the counters for all the
	 * consumers are higher than the counter set during the previous garbage collection it is time to do a new
	 * garbage collection.  The garbage collection can free the difference between the lowest number of finished
	 * consumer buffers and the number saved during the last garbage collection.
	 *
	 * Note this algorithm does go through the consumers list a couple of times but the list of consumers is typically
	 * very small (one in the vast majority of cases).
	 */
	private def doGC() {
		/* Find # of buffers that can be freed */
		val freeItems = getHowManyAreGarbage
		if (freeItems > 0) {
			/* Update # of buffers freed */
			incCounter(freeItems)
			/*
			 * If we want to keep any of the freed buffers we get them from the last consumer to minimize allocation
			 * of new ByteBuffer wrappers.  The last consumer is supposed to use the original ByteBuffer
			 * allocated but other consumers use duplicates.
			 *
			 * We put up to keepFree buffers on the free list and let the others be JVM garbage collected.
			 */
			val wantFree = keepFree - directAllocationFailures // If allocation failures keep less free
			if (freeQueue.length < wantFree) {
				val bufsToFree =
					lastConsumer.get.getFirstFreeBuffers(math.min(wantFree - freeQueue.length, freeItems))
				freeQueue ++= bufsToFree
			}
			/* Finally take the buffers out of all the consumers free lists. */
			consumers.foreach(_.freeBuffers(freeItems))
		}
	}

	/**
	 * Find the number of buffers that can be freed.  We look through all the consumers to find the difference
	 * between the lowest buffer number that a consumer is finished with vs. the buffer number that was last
	 * freed during garbage collection
	 *
 	 * @return # of buffers that can now be freed
	 */
	private def getHowManyAreGarbage = {
		consumers.foldLeft(Integer.MAX_VALUE)((free, consumer) => {
			val consumerFree = (consumer.counter.getCount - getCount).toInt
			math.min(free, consumerFree)
		})
	}

	/**
	 * Publish there's new data available.  The data is moved onto the transition queue of each consumer.
	 *
 	 * @param data buffer with new data
	 * @param eof true if this is the end of the data stream
	 */
	def publishData(data: ByteBuffer, eof: Boolean) { publishDataBuffer(data, eof) }

	/**
	 * Publish that we've reached the end of the data stream.
	 */
	def publishEndOfData() { publishDataBuffer(endOfDataBuffer, eof = true) }

	/**
	 * Publish some new data.  If we're publishing the end-of-data then we also start a scheduler that will check
	 * when all the data has been consumed so that we can shut down the producer and consumers when everyone is done.
	 *
	 * @param data buffer with new data
	 * @param eof true if this is the end of the data stream
	 */
	private def publishDataBuffer(data: ByteBuffer, eof: Boolean) {
		consumers.foreach(_.publishData(data, eof))
		/**
		 * It's the end of data so we check every 1/2 second if consumers are done.  When they are down we
		 * terminate the producer and consumers.
		 */
		if (eof) {
			def checkIfAllDone() {
				import system.dispatcher
				system.scheduler.scheduleOnce(500 milliseconds) {
					doGC()
					if (consumers.forall(!_.isInTransition)) {
						system.log.info(getCurrentReport(getReportIntro("a total of ")))
						if (directAllocationFailures > 0) {
							system.log.warning(
								"Direct buffer allocation failures: " + directAllocationFailures.toString)
						}
						terminate()
						consumers.foreach(_.stop())
					} else {
						checkIfAllDone()
					}
				}
			}
			checkIfAllDone()
		}
	}

	/**
	 * Terminate the producer - up to subclasses to decide what is needed.
	 */
	protected def terminate()

	/**
	 * Abstract method to get new consumer object - exactly what the new consumer is is determined by subclasses.
	 *
	 * @param consumerName name of consumer
	 * @param actor reference to actor being used for consumer
	 * @param last true if the last consumer (used to avoid duplicate of ByteBuffers for last consumer)
 	 */
	protected def getNewConsumer(consumerName: String, actor: ActorRef, last: Boolean) : ConsumerCore

	/**
	 * Type defined to allow subclasses of the ConsumerCore to be specified as a "Consumer".
 	 */
	type Consumer <: ConsumerCore

	/**
	 * Information about each consumer of the queue.  Consumers responsibilities center around maintaining the
	 * "transition" queue where data is placed when "published" for consumers to do what they want with.
	 *
 	 * @param name actor name
	 * @param consumer actor reference
	 * @param isLast true if the last consumer (used to get non-duplicate ByteBuffers)
	 */
	abstract class ConsumerCore(name: String, consumer: ActorRef, private[DataQueueCore] val isLast: Boolean) {

		/**
		 * Type defined to allow different consumer types to use different counters types.
 		 */
		type ConsumerCounter <: Counter

		/**
		 * Counter used to track # of last buffer we've finished using.  This counter is used during garbage collection.
		 */
		private[dataQueue] val counter : ConsumerCounter

		/**
		 * Type defined to allow different transition data types for different consumer types.
 		 */
		type TransitionData <: TransitionDataCore

		/**
		 * Buffer that's been published to consumer.
		 *
		 * @param data buffer
		 */
		abstract class TransitionDataCore(data: ByteBuffer, gcCount: Counter) {
			/**
			 * Set buffer as done - this method is normally only used if TransitionDataCore entries are used one at
			 * a time by the consumer.
			 */
			def allDone() {
				counter.incCounter(1)
			}

			/**
			 * Get counter used for this buffer.  Allows use of counter separate from TransitionDataCore.  This is the
			 * recommended usage of the counter so that lists of the ByteBuffers can be kept separate from
			 * the TransitionDataCore entries.  Then once usage of the data is complete the counter can be called
			 * just once for all the data used.  Note there's one counter per consumer used to mark the number of data
			 * buffers used by the consumer.
			 */
			def getCounter = gcCount

			/**
			 * Get data buffer.
			 *
			 * @return data buffer
			 */
			def getData = data

			/**
			 * Get the size of the data
 			 */
			def dataSize = data.limit()
		}

		/**
		 * Queue of transition data
 		 */
		private val transitionQueue = new mutable.ListBuffer[TransitionDataCore]

		/**
		 * Is any data still in transition for this consumer.
		 *
		 * @return true if any data is in use by consumer
		 */
		private[DataQueueCore] def isInTransition = !transitionQueue.isEmpty

		/**
		 * Stop the consumer - we just need to close down the counter
 		 */
		private[DataQueueCore] def stop() {	counter.close()	}

		/**
		 * Get a specified number of entries off of the top of the transition queue (FIFO).
		 *
 		 * @param howMany # of entries to remove
		 *
		 * @return buffers removed
		 */
		private[DataQueueCore] def getFirstFreeBuffers(howMany: Int) = {
			transitionQueue.dropRight(transitionQueue.size - howMany).map(_.getData)
		}

		/**
		 * Free buffer no longer in use
		 *
		 * @param howMany how many buffers to free
 		 */
		private[DataQueueCore] def freeBuffers(howMany: Int) {
			transitionQueue.remove(0, howMany)
		}

		/**
		 * Allocate a new object to use to set the buffer in transition.
		 *
 		 * @param buf buffer being put into transition
		 *
		 * @return buffer wrapped in a TransitionData object
		 */
		protected def getNewTransitionData(buf: ByteBuffer) : TransitionData

		/**
		 * Process data.  Tell consumer new data is available.
		 *
		 * @param data data to be processed
		 */
		protected def processData(data: TransitionData)

		/**
		 * Add a buffer to the transition queue and publish the new data to the consumers.  We duplicate it in a
		 * new ByteBuffer for each consumer except the last one so each consumer can use it in parallel (note it must
		 * be the last consumer that doesn't do a duplicate since if it was a previous one the ByteBuffer may have
		 * already been changed by the time it is duplicated for someone else).  The buffer is also flipped so the
		 * consumer(s) can write out data.  We don't duplicate the buffer for the last consumer to save on allocations
		 * of the ByteBuffer wrappers.  Since most applications have just a single consumer this saves a significant
		 * amount of unneeded allocations.
		 *
		 * @param buf buffer to add to queue
		 * @param eof true if buffer is end-of-data
		 */
		private[DataQueueCore] def publishData(buf: ByteBuffer, eof: Boolean) {
			def putInTransition(buffer: ByteBuffer) {
				val data = if (!isLast) buffer.duplicate() else buffer
				data.flip()
				val newItem = getNewTransitionData(data)
				transitionQueue += newItem
				processData(newItem)
			}

			putInTransition(buf)
			if (eof && !isEndOfDataBuffer(buf)) putInTransition(endOfDataBuffer)
		}

	}
}

/**
 *
 * Version of DataQueueCore used when the producer is one actor and each consumer is a separate actor.  In this case
 * we must do a few extra things to keep things thread safe.  In particular we must be aware that the consumers need
 * to update the buffer usage counter in a thread safe manner (via agents) and be told via messaging when new data
 * is available.
 *
 * @param producer data creator
 * @param consumerActors list of actors that will consumer the data
 * @param system active akka system that producer and consumers live under
 * @param bufferUsage optional size of buffers and max # of free buffers to keep cached
 */
class DataQueue(producer: ActorRef, consumerActors: Map[String, ActorRef], system: ActorSystem,
                bufferUsage: BufferSettings)
	extends DataQueueCore(producer, consumerActors, system, bufferUsage) {

	class Consumer(name: String, actor: ActorRef, last: Boolean) extends ConsumerCore(name, actor, last) {

		/**
		 * Agent used for keeping used buffer count
 		 */
		type ConsumerCounter = AgentCounter
		override private[dataQueue] val counter : ConsumerCounter = new AgentCounter(system){ }

		/**
		 * Transitional data - nothing different than the core one except that we need to specify what kind of
		 * counter is being used.
		 *
 		 * @param data buffer
		 * @param gcCounter consumers counter
		 */
		class TransitionData(data: ByteBuffer, gcCounter: AgentCounter)
		  extends TransitionDataCore(data, gcCounter) { }

		/**
		 * Allocate a new object to use to set the buffer in transition.
		 *
 		 * @param buf buffer being put into transition
		 *
		 * @return buffer wrapped in a TransitionData
		 */
		override protected def getNewTransitionData(buf: ByteBuffer) =
			new TransitionData(buf, counter)

		/**
		 * Process data.  We send out a message to the consumer.
		 *
		 * @param data data to be processed
		 */
		override protected def processData(data: TransitionData) { actor ! DataToProcess(data) }
	}

	/**
	 * Terminate the producer
 	 */
	override protected def terminate() {
		if (!producer.isTerminated) system.stop(producer)
	}

	/**
	 * Allocate a new object representing a consumer
	 *
	 * @param consumerName name of consumer
	 * @param actor actor receiving messages for consumer
	 * @param last true if the last consumer (used to avoid duplicate of ByteBuffers for last consumer)
	 *
	 * @return object with consumer info
	 */
	override protected def getNewConsumer(consumerName: String, actor: ActorRef, last: Boolean) =
		new Consumer(consumerName, actor, last)
}

/**
 * Producer and consumer are a single actor - there's no need for an Agent to track usage and exactly how the data
 * is processed is up to the producer/consumer.
 *
 * @param producer data producer
 * @param system active akka system used by producer
 * @param bufferUsage optional size of buffers and max # of free buffers to keep cached
 */
abstract class DataQueueLinkedConsumer(producer: ActorRef, system: ActorSystem,
                                       bufferUsage: BufferSettings)
	extends DataQueueCore(producer, Map[String, ActorRef](producer.path.name -> producer), system, bufferUsage) {

	/**
	 * Method that must be provided to handle the data being processed.  We could just send a message to the
	 * consumer but since the producer and consumer are the same for DataQueueLinkedConsumer it can be more
	 * efficient to just call back to a supplied method to process the data.
	 *
	 * @param data transition data being processed
	 */
	protected def dataProcessor(data: DataQueueLinkedConsumer#Consumer#TransitionData)

	class Consumer(name: String, actor: ActorRef, last: Boolean) extends ConsumerCore(name, actor, last) {

		/**
		 * "Simple" counter can be used since producer and consumer are the same so things are thread-safe.
 		 */
		type ConsumerCounter = SimpleCounter
		override private[dataQueue] val counter : ConsumerCounter = new SimpleCounter{ }

		/**
		 * Transitional data - nothing different than the core one except that we need to specify what kind of
		 * counter is being used.
		 *
 		 * @param data buffer
		 * @param gcCounter consumers counter
		 */
		class TransitionData(data: ByteBuffer, gcCounter: SimpleCounter)
		  extends TransitionDataCore(data, gcCounter) { }

		/**
		 * Allocate a new object to use to set the buffer in transition.
		 *
 		 * @param buf buffer being put into transition
		 *
		 * @return buffer wrapped in a TransitionData
		 */
		override protected def getNewTransitionData(buf: ByteBuffer) =
			new TransitionData(buf, counter)

		/**
		 * Go process the data - exactly what happens is determined by the subclass.
		 *
 		 * @param data data to be processed
		 */
		override protected def processData(data: TransitionData) { dataProcessor(data) }
	}

	/**
	 * Nothing we need to do to terminate the producer - it takes care of terminating itself
 	 */
	override protected def terminate() { }

	/**
	 * Allocate a new object representing a consumer
	 *
	 * @param consumerName name of consumer
	 * @param actor actor receiving messages for consumer
	 * @param last true if the last consumer (used to avoid duplicate of ByteBuffers for last consumer)
	 *
	 * @return object with consumer info
	 */
	override protected def getNewConsumer(consumerName: String, actor: ActorRef, last: Boolean) =
		new Consumer(consumerName, actor, last)
}

/**
 * Some defaults and messages used by DataQueue
 */
object DataQueue {
	/**
	 * Message sent from producer to consumer to say there's new data available for processing (i.e., "in transition")
	 *
	 * @param data ConsumerCore with data
	 */
	case class DataToProcess(data: DataQueue#Consumer#TransitionData)

	/**
	 * Is buffer end-of-data marker?  Can't do simple "==" since we do duplicates of ByteBuffers, including the
	 * endOfData.  However the endOfData is the only buffer we use that is not direct and just to make real sure
	 * we check that the capacity is 0.
	 *
	 * @return true if buffer is end-of-data marker
	 */
	def isEndOfDataBuffer(buffer: ByteBuffer) = !buffer.isDirect && buffer.capacity == 0

}
