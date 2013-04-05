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

import akka.actor._
import collection.mutable
import scala.Some
import akka.remote.RemoteClientShutdown
import org.broadinstitute.PEMstr.common.util.Util
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.WorkflowDefinition
import org.broadinstitute.PEMstr.common.workflow.WorkflowMessages.{AbortWorkflow, WhatIsWorkflowState,
StepState, WorkflowState, WorkflowDone, NewStep, ExecutionState}
import ExecutionState.UNKNOWN
import org.broadinstitute.PEMstr.common.CentralSchedulerMessages._
import org.broadinstitute.PEMstr.common.ActorWithContext._
import org.broadinstitute.PEMstr.common.ActorWithContext
import org.broadinstitute.PEMstr.common.workflow.WorkflowSplitJoin.splitWorkflow

/**
 * @author Nathaniel Novod
 * Date: 8/22/12
 * Time: 4:28 PM
 *
 * This is the beginning of communication with the outside work.  This actor accomplishes two things:
 * - Receives messages from remote nodes to startup execution of a workflow
 * - Receives messages from remote step executors to be put on the subscription list to know when steps are available
 * for execution.  Someday (perhaps in akka 2.1 with clusters) it may be possible to subscribe to an event bus
 * remotely.  However, for now this actor must handle the "subscriptions" via normal remote messaging.
 */
class CentralScheduler extends ActorWithContext {
	/**
	 * Actor context - it is saved in instance between invocations of actor and also restored to any new instances
	 * of the actor that get created due to restarts.
	 */
	protected class LocalContext extends MutableContext {
		/* bus on which we are publishing */
		val eventBus = new StepExecutionRequestBus

		/* actors (kept as refs which can be refs to remote actors) that are subscribing to the bus */
		val subscribers = mutable.HashMap.empty[String,  List[ActorRef]]

		/* actors managing workflows */
		val workflows = mutable.HashMap.empty[String,  ActorRef]
	}

	/**
	 * What our context looks like
 	 */
	override type MyContext = LocalContext

	/**
	 * Create a new context.
	 *
	 * @return new instance of local context
	 */
	override protected def getMyContext = new LocalContext

	/**
	 * Executed before the actor starts up.  We subscribe to any remote client shutdowns so we can take them off the
	 * subscription list.
	 * Need to look out for other events? For example:
	 * context.system.eventStream.subscribe(self, classOf[RemoteLifeCycleEvent])
	 */
	override def preStart() {
		context.system.eventStream.subscribe(self, classOf[RemoteClientShutdown])
	}

	/**
	 * Called after an actor has stopped - we remove ourselves from subscriptions
	 */
	override def postStop() {
		context.system.eventStream.unsubscribe(self, classOf[RemoteClientShutdown])
	}

	/**
	 * Define heart of the actor which receives messages about new workflows and publishes when steps are available
	 * for execution.
	 */
	def receive = {
		/*
		 * A request to execute a new workflow.  We hand off the work to a new actor that will manage the execution of
		 * the workflow.  Before handing off the workflow we change it so that split/join paths are made into
		 * multiple steps/flows as needed.  Once the workflow is handed off to the workflow manager the splits/joins
		 * become largely transparent.  Someday we can dynamically adjust the level of splitting/joining
		 * here based on available resources but for now the level is set in the split definitions that are part
		 * of the workflow definition.
		 *
		 * wf: Definition for workflow
		 */
		case StartWorkflow(wf: WorkflowDefinition) => {
			val workflowID = Util.getTime
			val wfName = "wf_" + wf.name + "_" + Util.getTimeID(workflowID)
			log.info("Starting workflow manager for " + wfName +
				" logging to " + wf.logDir + " using temp directory " + wf.tmpDir)
			val wfActor = context.actorOf(Props(new WorkflowManager(splitWorkflow(wf), workflowID,
				locals.eventBus, self)), name = wfName)
			locals.workflows += (wfName -> wfActor)
		}

		/*
		 * Upon shutdown of remote client make sure any subscriptions from client are eliminated
		 *
		 * transport: akka RemoteTransport
		 * address: address of actor shutting down
 		 */
		case RemoteClientShutdown(transport, address) =>  {
			log.info("RemoteClientShutdown of " + address)
			locals.subscribers.get(address.toString) match {
				case Some(actors) => {
					actors.foreach((actor: ActorRef) => {
						log.info("Unsubscribing: " + actor)
						locals.eventBus.unsubscribe(actor)
					})
					locals.subscribers.remove(address.toString)
				}
				case None =>
			}
		}

		/*
		 * Someone wants to subscribe to notifications of new jobs (NewStep).
		 *
		 * subscriber: akka actor ref of subscriber
		 */
		case Subscribe(subscriber) => {
			log.info("received subscription from: " + subscriber)
			locals.eventBus.subscribe(subscriber, classOf[NewStep])
			/*
			 * Get remote address of actor and save actor ref under address.  Later, if the we get a shutdown
			 * from the address we know to unsubscribe all assocated actors.
 			 */
			val subAddress = subscriber.path.address
			val subList = locals.subscribers.getOrElse(subAddress.toString, {
				List.empty[ActorRef]
			})
			locals.subscribers += (subAddress.toString -> (subscriber :: subList))
		}

		/*
		 * The workflow is done
		 *
		 * name: name of workflow
		 * status: completion status of workflow
		 */
		case WorkflowDone(name, status) => {
			log.info("workflow " + name + " completed with status: " + status.toString)
			locals.workflows.remove(name)
		}

		/*
		 * Get a list of active workflows
 		 */
		case ListWorkflows => sender! WorkflowList(locals.workflows.keySet.toSet)

		/*
		 * Get the status of a workflow
		 *
		 * name: name of workflow
		 */
		case WorkflowStatus(name: String) => {
			locals.workflows.get(name) match {
				case Some(actor) => actor ! WhatIsWorkflowState(sender)
				case None => sender ! WorkflowState(UNKNOWN, List.empty[StepState])
			}
		}

		/*
		 * Abort a workflow
		 *
		 * name: name of workflow
		 */
		case WorkflowAbort(name: String) => {
			locals.workflows.get(name) match {
				case Some(actor) => actor ! AbortWorkflow
				case None =>
			}
		}

		/*
		 * Unknown message
 		 */
		case e => log.warning("received unknown message: " + e)
	}
}
