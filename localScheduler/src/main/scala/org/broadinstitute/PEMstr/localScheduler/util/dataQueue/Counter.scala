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

import akka.agent.Agent
import akka.actor.ActorSystem

/**
 * @author Nathaniel Novod
 * Date: 1/26/13
 * Time: 9:46 AM
 *
 * Simple trait to maintain a counter.  You can increment it, retrieve it and "close" it.  The close is needed
 * for some fancy counters, based on akka Agents, which must be closed when we're done using them.
 */
sealed trait Counter {
	/**
	 * Add to the counter
	 * @param increment amount to add
	 */
	def incCounter(increment: Int)

	/**
	 * Get the current contents of the counter.
	 *
	 * @return counter contents
	 */
	def getCount : BigInt

	/**
	 * "Close" the counter - up to implementations exactly what that means
	 */
	def close()
}

/**
 * Class based on counter trait that uses an akka agent for the counter so that multiple threads can update the counter.
 * Note that reads of agents are immediate but writes take place in separate threads that insure that no writes are lost
 * but do not insure exactly when the update will occur (e.g., you can do a read of a counter that has an outstanding
 * update that is not included)
 *
 * @param system akka system to use for agent
 */
class AgentCounter(system: ActorSystem) extends Counter {
	/**
	 * Counter.
	 */
	private val count = Agent[BigInt](0)(system)

	/**
	 * Add to the counter.
	 *
	 * @param increment amount to add
	 */
	def incCounter(increment: Int) {count send {_ + increment}}

	/**
	 * Retrieve count.
	 *
 	 * @return counter contents
	 */
	def getCount = count()

	/**
	 * Cloase the counter - shut down the agent.
	 */
	def close() { count.close() }
}

/**
 * A simple counter trait.  This counter is not thread safe.
 */
trait SimpleCounter extends Counter {
	/**
	 * Count - a simple var since we don't have to worry about multi threading.
	 */
	private var count = BigInt(0)

	/**
	 * Add to counter.
	 * @param increment amount to add
	 */
	def incCounter(increment: Int) { count += increment }

	/**
	 * Retrieve counter.
	 *
	 * @return counter contents
	 */
	def getCount = count

	/**
	 * Close counter - nothing to do for a simple counter.
 	 */
	def close() { }
}
