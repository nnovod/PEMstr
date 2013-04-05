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

package org.broadinstitute.PEMstr.common
import scala.collection.mutable
import akka.actor.{ActorLogging,Actor}
import org.broadinstitute.PEMstr.common.ActorWithContext.{MutableContext,contextMap}

/**
 * @author Nathaniel Novod
 * Date: 12/31/12
 * Time: 1:55 PM
 *
 * Used to save the context of an actor.  The first time an actor is instantiated a new context is created via a
 * callback to the actor instance.  After that, if the actor is stopped and restarted, a new instance is created
 * with the context saved (in preRestart) and then restored (via new setup of locals).
 *
 */
trait ActorWithContext extends Actor with ActorLogging {
	// @TODO Use this trait in all actors? - also need recovery strategy for children
	/**
	 * Type to be set by subclass to actual context class
 	 */
	type MyContext <: MutableContext

	/**
	 * Callback to subclass to create initial instance of context
	 * @return
	 */
	protected def getMyContext : MyContext

	/**
	 * Where context is set - first time it will call getMyContext to get a new context.  After that, upon restart,
	 * it will get the context from the map where it was saved during preRestart.
	 */
	protected val locals  = contextMap.remove(self.path.name) match {
		case Some(c: MutableContext) => c.asInstanceOf[MyContext]
		case _ => getMyContext
	}

	/**
	 * Called while actor is still active but is about to be restarted - we save the context so when it is restarted
	 * with a new instance the context from the previous instance is picked up.
 	 */
	override def preRestart(reason: Throwable, message: Option[Any]) {
		log.info("Restarting")
		contextMap += (self.path.name -> locals)
		super.preRestart(reason, message)
	}
}

/**
 * Matching object with map that saves context when a restart of the actor takes place.  Note that the hashmap should
 * be used infrequently: Only when an actor is restarting.
 */
object ActorWithContext {
	trait MutableContext
	val contextMap = new mutable.HashMap[String, MutableContext] with mutable.SynchronizedMap[String, MutableContext]
}
