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

package org.broadinstitute.PEMstr.centralScheduler

import akka.event.{LookupClassification, ActorEventBus}
import akka.actor._
import org.broadinstitute.PEMstr.common.workflow.WorkflowMessages.NewStep

/**
 * @author Nathaniel Novod
 * Date: 8/16/12
 * Time: 1:02 PM
 *
 * Class to make a "bus" to publish step execution request messages.  This bus is setup for NewStep events.  As of now
 * a bus can not be visible remotely.  Consequently when remote actors want to subscribe to this bus
 * (e.g., a LocalScheduler) they must send a Subscribe message to the CentralScheduler which will then subscribe
 * the sender to the bus.
 *
 * Note that this is an extension of the ActorEventBus which sets up a bus with actors as subscribers and
 * LookupClassification which allows subscribers to look for messages based on a classification (Classifier).
  */
class StepExecutionRequestBus extends ActorEventBus with LookupClassification {
	/**
	 * Event to subscribe to
	 */
	type Event = NewStep

	/**
	 * Classify events based on class
	 */
	type Classifier = Class[_]

	/**
	 * Classify an event - we simply get it's class and use it as the classification.
	 *
	 * @param event event to be classified
	 *
	 * @return classification (event's class)
	 */
	def classify(event: Event) = event.getClass

	/**
	 * Size of map with classifications
	 */
	protected def mapSize() = 1 // Expected to only have one classification

	/**
	 * Publish an event
	 *
	 * @param event event to publish
	 * @param subscriber subscriber to send event message to
	 */
	def publish(event: Event, subscriber: Subscriber) {subscriber ! event}
}
