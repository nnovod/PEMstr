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

package org.broadinstitute.PEMstr.localScheduler

import akka.actor._
import LocalScheduler.YourStep
import step.StepManager
import org.broadinstitute.PEMstr.common.workflow.WorkflowMessages.{WorkflowID,StepDone,ItsYours,WantStep,NewStep,NeverMind}
import org.broadinstitute.PEMstr.common.util.Util.{plural,getTimeID}
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.{StepDefinition, StepResources}
import concurrent.ExecutionContext
import java.util.concurrent.Executors

/**
 * @author Nathaniel Novod
 * Date: 8/16/12
 * Time: 2:01 PM
 *
 * Receives requests (NewStep) to execute steps and then responds (WantStep) if it wants them, finally
 * possibly acquiring them (ItsYours) to execute.
 *
 * @param cpus max # of cpus to use
 * @param gigs max # of gigs of memory to use
 * @param ID identifier
 */
class LocalScheduler(cpus: Int, gigs: Int, ID: String) extends Actor with ActorLogging {
	/**
	 * Method called before actor starts - we just subscribe to dead letters to know about steps that die.
	 */
	override def preStart() {
		context.system.eventStream.subscribe(self, classOf[DeadLetter])
		log.info("Starting LocalScheduler ID " + ID + " for " + plural(cpus, "cpu") + " and " + plural(gigs, "gig"))
	}

	/**
	 * States of steps - we want to execute or we actually are executing
	 */
	private object StepState extends Enumeration {
		val WANT, OURS = Value
	}
	import StepState._

	/**
	 * State of steps we're executing
	 *
 	 * @param state state of step execution
	 * @param cores # of cores associated with step
	 * @param gbMem # of gigabytes of memory associated with step
	 */
	private case class StepUsage(state: StepState.Value, cores: Int, gbMem: Int)

	/* Map with executing states and their state */
	private val stepStates = collection.mutable.HashMap.empty[String, StepUsage]

	/**
	 * Get unique step ID
	 * @param name name of step
	 * @param wfID workflow ID including startup time (time since "epoch")
	 *
	 * @return unique ID
	 */
	private def getStepID(name: String, wfID: WorkflowID) = wfID.name + "_" + name + "_" + getTimeID(wfID.timeID)

	/**
	 * Get resources in use by steps
 	 * @return (# of cores in use, # of gigabytes in use)
	 */
	private def resourcesInUse = stepStates.foldLeft(0,0) ((tot, kv) => (tot._1 + kv._2.cores, tot._2 + kv._2.gbMem))

	/**
	 * Get resources to be used by step
 	 * @return (# of cores to use, # of gigabytes to use)
	 */
	private def resourcesToUse(resources: StepResources) =
		(resources.coresWanted.getOrElse(1), resources.gbWanted.getOrElse(1))

	/**
	 * This is our poor man's scheduler.  We decide whether or not we will ask for the job based on the following:
	 *
	 * - If it is targeted for us then we stretch our resources (allow the scheduler to take on up to 50% above
	 * resources initially assigned to scheduler) to try to take the step.  If the stretch still isn't enough then we
	 * don't offer to execute the step.
	 *
	 * - If it is targeted for another scheduler then we ask for it if there have been at least 2 previous attempts
	 * to get a taker and we have the resources available.
	 *
	 * - It it is not targeted for a particular scheduler then we ask for it if we have the resources available.
	 *
 	 * @param resources resources wanted for step
	 * @param attempt # of previous attempts to get someone to take this job
	 *
	 * @return true if we should request the job
	 */
	private def isStepToTake(resources: StepResources, attempt: Int) = {
		def isForUs = resources.suggestedScheduler.isDefined && resources.suggestedScheduler.get == ID

		def schedulerResources =
			if (isForUs) (cpus + cpus/2, gigs + gigs/2) else (cpus, gigs)

		if (isForUs || resources.suggestedScheduler.isEmpty || attempt > 2) {
			val (usedCores, usedGigs) = resourcesInUse
			val (schedulerCores, schedulerGigs) = schedulerResources
			val (wantedCores, wantedGigs) = resourcesToUse(resources)
			schedulerCores - usedCores >= wantedCores && schedulerGigs - usedGigs >= wantedGigs
		} else false
	}

	/**
	 * Heart of actor - receive and respond to messages
	 */
	def receive = {
		/*
		 * Message to say that a new job (step) is available for execution.  If we have the resources we grab it.
		 *
		 * name: name of step
		 * wfID: ID of workflow
		 * resources: resources wanted for step
		 * attempt: number of attempts already made to get a taker for this step
		 * replyTo: reference to actor to send reply to
		 */
		case NewStep(name, wfID, resources, attempt, replyTo) => {
			val stepID = getStepID(name, wfID)
			if (stepStates.get(stepID).isEmpty) {
				if (isStepToTake(resources, attempt)) {
					val (cores, gbMem) = resourcesToUse(resources)
					stepStates += (stepID -> StepUsage(WANT, cores, gbMem))
					replyTo ! WantStep(name, wfID, ID)
				}
			} else {
				log.warning("Ignoring NewStep request for step already taken: " + stepID)
			}
		}

		/*
		 * A step we asked to execute was not assigned to us.  We remove it from the list of executing steps.
		 *
		 * name: name of step
		 * wfID: ID of workflow
		 */
		case NeverMind(name, wfID) => stepStates -= getStepID(name, wfID)

		/*
		 * It's your lucky day - you've been given the job you wanted.  We startup a new StepManager actor to handle
		 * the job.
		 *
		 * name: step name
		 * wfID: ID of workflow
		 * tempDir: workflow temporary directory specification
		 * logDir: logging directory specification
		 * tokens: tokens to be replaced in command args
		 * stepDef: step definition
		 */
		case ItsYours(name, wfID, tempDir, logDir, tokens, stepDef) => {
			val stepID = getStepID(name, wfID)
			val (cores, gbMem) = resourcesToUse(stepDef.resources)
			stepStates += (stepID -> StepUsage(OURS, cores, gbMem))
			val jobActor = context.actorOf(Props[StepManager], name = stepID)
			context.watch(jobActor)
			jobActor ! YourStep(name, wfID, tempDir, logDir, stepDef, tokens, sender)
		}

		/*
		 * StepDone - step manager says it is done executing step - we remove it from the list of executing steps
		 *
		 * name - step name
		 * completionStatus - integer completion status for step
 		 */
		case StepDone(name, completionStatus) => {
			log.info(name + " reported done with status: " + completionStatus.toString)
			stepStates -= sender.path.name
		}

		/*
		 * Step actor ended - we remove it from the list of executing steps (in cae StepDone never done)
		 *
		 * child: actor ref to step actor terminated
 		 */
		case Terminated(child) => {
			val stepID = child.path.name
			stepStates -= stepID
			log.info(stepID + " terminated")
		}

		/*
		 * Dead letter received - just record it's being lost.
		 *
		 * message: message that died
		 * sender: actor ref message sent from
		 * recipient: actor ref message sent to
		 */
		case DeadLetter(message, sender, recipient) ⇒ log.warning("Dead letter: " + message.toString +
			" from " + sender.path.name + " to " + recipient.path.name)

		/*
		 * Unknown message
		 */
		case e => log.warning("received unknown message: " + e)
	}
}

/**
 * Actor messages sent from local scheduler
 */
object LocalScheduler {
	/**
	 * Message sent to StepManager to tell about the step it is to execute.
	 *
	 * @param stepName step name
	 * @param wfID time ID of containing workflow
	 * @param tempDir workflow temporary directory file specification
	 * @param logDir workflow log directory file specification
	 * @param stepDef step definition
	 * @param tokens map of token names to token values
	 * @param respondTo reference of actor to respond (not us - respond to remote workflowManager)
	 *
	 */
	case class YourStep(stepName: String,  wfID: WorkflowID, tempDir: String, logDir: String, stepDef: StepDefinition,
	                    tokens: Map[String, String], respondTo: ActorRef)

	/**
	 * Create an execution context for futures that are doing IO - we want a separate context so that actor threads
	 * are not used for blocking requests.
 	 */
	implicit val futureExecutor = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
}

