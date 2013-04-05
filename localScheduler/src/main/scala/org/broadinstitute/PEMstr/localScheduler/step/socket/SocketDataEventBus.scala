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

package org.broadinstitute.PEMstr.localScheduler.step.socket

import akka.event.{LookupClassification, ActorEventBus}
import akka.actor._
import SocketDataEventBus.SocketData

/**
 * @author Nathaniel Novod
 * Date: 10/7/12
 * Time: 4:28 PM
 *
 * Class to make a "bus" to publish socket messages.  This bus is setup for SocketData events.
 * Actors can subscribe for any type of SocketData (the classifier is a class).  Note that this is an extension of the
 * ActorEventBus which sets up a bus with actors as subscribers and LookupClassification which allows subscribers to
 * look for messages based on a classification (Classifier).
 */
class SocketDataEventBus extends ActorEventBus with LookupClassification {
	/**
	 * Type of event to look for
	 */
	type Event = SocketData

	/**
	 * Classifier - classification done by class
	 */
	type Classifier = Class[_]

	/**
	 * Classify an event by its class.
	 *
	 * @param event event to be classified
	 *
	 * @return classification (event's class)
	 */
	def classify(event: Event) = event.getClass

	/**
	 * Size of map with classifications
	 */
	protected def mapSize() = 2 // Expected to have two classifications (Data received/sent and Close)

	/**
	 * Publish an event
	 *
	 * @param event event to publish
	 * @param subscriber subscriber to send event message to
	 */
	def publish(event: Event, subscriber: Subscriber) {subscriber ! event}
}

object SocketDataEventBus {
	/**
	 * Marker for messages to be published on this bus
	 */
	trait SocketData
}
