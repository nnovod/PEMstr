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
import collection.immutable.HashMap
import org.broadinstitute.PEMstr.common.workflow.WorkflowMessages._
import ExecutionState._
import org.broadinstitute.PEMstr.common.util.{StartMapTracker,ValueMapTracker}
import org.broadinstitute.PEMstr.common.util.Util.getTimeID
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions._
import scala.concurrent.duration._
import org.broadinstitute.PEMstr.common.StepCommandArgTokens.{replaceVariables, getVariableValue}

/**
 * @author Nathaniel Novod
 * Date: 8/16/12
 * Time: 1:31 PM
 *
 * Top level actor for a workflow - it is setup with a workflow definition and then manages the assignment of the
 * steps within the workflow to remote actors.
 *
 * @param workflow definition of the workflow to be executed
 * @param timeID epoch time workflow instance created
 * @param bus bus onto which to publish availability of new steps to execute
 * @param replyTo actor to tell about workflow status
 */
class WorkflowManager(workflow: WorkflowDefinition, timeID: Long,
                      bus: StepExecutionRequestBus, replyTo: ActorRef) extends Actor with ActorLogging {
	/**
	 * Message to say to republish untaken steps
 	 */
	private case class Publish(attempt: Int)

	/**
	 * Go advertise to actors subscribed to wanting to know about steps to be executed.
 	 */
	override def preStart() {
		self ! Publish(0)
	}

	/* Get workflow specific temporary directory (tempDir with workflow actor name added) */
	private lazy val wfTempDir = workflow.tmpDir + "/" + self.path.name

	/* Get workflow specific temporary directory (tempDir with workflow actor name added) */
	private lazy val wfLogDir = workflow.logDir + "/" + self.path.name

	/**
	 * Tracks the remote actor associated with a step and the sockets being used to send input to the step
	 *
	 * @param actor reference to remote actor executing step
	 * @param stepName name of step being executed
	 * @param host name of remote node executing step
	 * @param inputPorts socket ports used for inputs
	 *
	 */
	private case class SocketActor(actor: ActorRef, stepName: String, host: String,
	                               inputPorts: Map[String, InputConnectionInfo])

	/* Get list of step names */
	private val stepNames = workflow.steps.keys.toList

	/* Record which steps have been allocated for execution */
	private val stepsTaken = new ValueMapTracker[String, String](stepNames)

	/* Record actors that are executing steps */
	private val stepActors = new ValueMapTracker[String, SocketActor](stepNames)

	/* Record which steps have setup inputs */
	private val stepsInputs = new StartMapTracker(stepNames)

	/* Record which steps have started executing */
	private val stepsStarted = new StartMapTracker(stepNames)

	/* Record which steps have completed executing */
	private val stepsDone = new ValueMapTracker[String, Int](stepNames)

	/* Create a map with output files as key and the set of input files (steps) they feed as values */
	private lazy val outputMap = {
		/* Go through the steps to build up the map */
		workflow.steps.foldLeft(HashMap.empty[DataFilePointer, Set[String]])(
			(totalMap, stepEntry) => {
				val (stepName, stepDef) = stepEntry
				val inputData = stepDef.inputs
				/* Go through the inputs for this step finding their sources (outputs that feed them) */
				inputData.foldLeft(totalMap)(
					(stepMap, inputEntry) => {
						val (_, inputData) = inputEntry
						val source = inputData.sourceData
						stepMap.get(source) match {
							case Some(soFar) => stepMap + (source -> (soFar + stepName))
							case None => stepMap + (source -> Set(stepName))
						}
					})
			})
	}

	/**
	 * Create a dataflow containing information for how data will go between two steps.  The returned object
	 * is either for a pipe or socket to be used between the steps.
	 *
	 * @param outputSource pointer to source file for flow
	 * @param inputTarget pointer to target file for flow
	 * @param logDirectPipe log if a direct pipe is being used for dataflow
	 *
	 * @return Information about pipe or socket to be used between the steps
	 */
	private def getDataFlowConnection(outputSource: DataFilePointer,
	                                  inputTarget: DataFilePointer, logDirectPipe: Boolean = false) : DataFlow =
	{
		/**
		 * Get the # of places a step's output data is going to.
		 *
		 * @param outputSource pointer to output source
		 *
		 * @return number of inputs to receive output
		 */
		def inputSizeForOutput(outputSource: DataFilePointer) =
			outputMap.get(outputSource) match {
				case Some(inputs) => inputs.size
				case None => 0
			}

		/**
		 * Get the host name for a step
		 *
		 * @param stepName name of step
		 *
		 * @return name of node step is executing on
		 */
		def getHost(stepName: String) = stepActors(stepName).host

		/**
		 * Get the actor managing a step
		 *
		 * @param stepName name of step
		 *
		 * @return reference for actor managing step
		 */
		def getStepActor(stepName: String) = stepActors(stepName).actor

		/**
		 * Can this be a direct pipe
		 *
 		 * @param output pointer to output file
		 * @return true if this can not be a direct pipe
		 */
		def isNotDirectPipe(output: DataFilePointer) =
			workflow.steps(output.stepName).outputs(output.dataName).isNotDirectPipe

		/**
		 * Is output checkpointed
		 *
 		 * @param output pointer to output file
		 * @return true if this can not be a direct pipe
		 */
		def isCheckpointed(output: DataFilePointer) =
			workflow.steps(output.stepName).outputs(output.dataName).checkpointAs.isDefined

		/**
		 * Will input be "rewound"
		 *
		 * @param input pointer to input to be checked
		 *
		 * @return true if the input will need to be "rewound"
 		 */
		def isRewind(input: DataFilePointer) =
			workflow.steps(input.stepName).inputs(input.dataName).stream.rewind.isDefined

		/**
		 * Determine if the link between output of one step to input of another step can be a direct pipe without the
		 * need for sockets.  If the output is going to a single input and the output and input steps are on the same
		 * node and the input will not be rewound and the user didn't say that direct pipes should not be used then
		 * it can be a direct pipe.  Otherwise sockets are used.
		 *
		 * @param outputSource pointer to output source
		 * @param inputTarget pointer to input target
		 *
		 * @return true if a direct pipe can be used between the source and target
		 */
		def isPipe(outputSource: DataFilePointer, inputTarget: DataFilePointer) = {
			inputSizeForOutput(outputSource) == 1 &&
				getHost(outputSource.stepName) == getHost(inputTarget.stepName) &&
				!isRewind(inputTarget) && !isNotDirectPipe(outputSource) && !isCheckpointed(outputSource)
		}

		/**
		 * Get the hidden name of the input pipe, if one is specified.
		 *
		 * @param output pointer to pipe output
		 *
		 * @return optional specification to be used for pipe
		 */
		def getHiddenPipeName(output: DataFilePointer) = {
			workflow.steps(output.stepName).outputs(output.dataName).hiddenAs match {
				case Some(hiddenAs) => Some(doTokenSubstitution(hiddenAs, output.stepName))
				case None => None
			}
		}

		/**
		 * Get the name of the pipe to be used for direct pipe communication between steps.  If the output file
		 * specification was specified as a hidden output then use that.  Otherwise use the specification assigned
		 * to the input pipe.
		 *
		 * @param input connection information including default pipe specification
		 * @param output pointer to pipe output
		 *
		 * @return specification to be used for pipe
		 */
		def getPipeName(input: InputConnectionInfo, output: DataFilePointer) = {
			getHiddenPipeName(output).getOrElse(input.pipe)
		}

		/**
		 * Get the server socket port for input data.
		 *
		 * @param input pointer to input
		 *
		 * @return socket port # to be used for input server socket
		 */
		def getInputPort(input: DataFilePointer) =
			stepActors(input.stepName).inputPorts(input.dataName)

		/* Get socket port of input data */
		val inputData = getInputPort(inputTarget)
		/* If a pipe is used then return the pipe information, otherwise return the socket information */
		if (isPipe(outputSource, inputTarget)) {
			if (logDirectPipe)
				log.info("Using direct pipe for " + outputSource.dataName +
				  " from step " + outputSource.stepName + " to step " + inputTarget.stepName)
			DataFlowPipe(inputTarget.dataName, getPipeName(inputData, outputSource))
		} else
			DataFlowSocket(inputTarget.dataName,
				SocketConnectionInfo(getHost(inputTarget.stepName), inputData.port), getStepActor(outputSource.stepName))
	}

	/**
	 * Substitute tokens in a string.
	 *
	 * @param input input string
	 * @param stepName step token is being set for
	 *
	 * @return string with any token names replaces with the token values
	 */
	private def doTokenSubstitution(input: String, stepName: String) : String = {
		replaceVariables(input, (tokenValue) =>
			getVariableValue(workflow.steps(stepName), workflow.tokens, WorkflowID(workflow.name, timeID), tokenValue))
	}

	/**
	 * Substitute tokens in a string.
	 *
	 * @param input input string
	 * @param stepName step token is being set for
	 *
	 * @return string with any token names replaces with the token values
	 */
	private def doTokenSubstitution(input: Option[String], stepName: String) : Option[String] = {
		if (input.isEmpty) None else
			Some(doTokenSubstitution(input.get, stepName))
	}

	/**
	 * Get step actor that is root of flow.
	 *
	 * @param flow name of output flow
	 * @param step name of step that is receiving data
	 */
	private def getFlowSourceActor(flow: String, step: String) = {
		if (!workflow.steps.get(step).isDefined || !workflow.steps(step).inputs.get(flow).isDefined) {
			None
		} else {
			val sourceStep = workflow.steps(step).inputs(flow).sourceData.stepName
			if (stepActors.isSet(sourceStep)) {
				Some(stepActors(sourceStep).actor)
			} else None
		}
	}

	/**
	 * Send a message to the source for a flow to pause/resume sending data
	 *
 	 * @param action pause or resume
	 * @param flow name of data flow
	 * @param step name of step that is receiving data
	 * @param msg pause or resume message
	 */
	private def changeSourceFlowFlow(action: String, flow: String, step: String, msg: FlowFlow) {
		getFlowSourceActor(flow, step) match {
			case Some(actor) => {
				log.info(sender.path.name + " request to " + action + " flow " + flow +
				  " being sent to " + actor.path.name)
				actor ! msg
			}
			case None => log.warning("Unable to " + action + " flow " + flow + ", flow not found")
		}
	}

	/**
	 * Receive messages used to assign steps in a workflow
	 */
	def receive = {
		/*
		 * Go advertise to actors subscribed to wanting to know about steps to be executed.
		 *
		 * attempt - # of attempts made so far to publish step's availability
	 	 */
		case Publish(attempt) => {
			for (stepName <- stepNames if !stepsTaken.isSet(stepName)) {
				val step = workflow.steps(stepName)
				val resources = StepResources(step.resources.coresWanted, step.resources.gbWanted,
					doTokenSubstitution(step.resources.suggestedScheduler, stepName), step.resources.consumption)
				log.info("Publishing request to execute " + stepName +
				  " (cores: " + resources.coresWanted + ", gigs: " +
				  resources.gbWanted + ", targetID: " + resources.suggestedScheduler +
				  ", attempt: " + attempt + ")")
				bus.publish(NewStep(stepName, WorkflowID(workflow.name, timeID), resources, attempt, self))
			}
			if (!stepsTaken.isAllSet) {
				import context.dispatcher
				context.system.scheduler.scheduleOnce(3000 milliseconds) {
					self ! Publish(attempt + 1)
				}
			}
		}
		/*
		 * WantStep - answer from remote manager that it wants to execute a workflow step.
		 *
		 * name - step name
		 * wfID - ID of workflow
		 * ID - ID of scheduler that wants step
		 */
		case WantStep(name, wfID, schedID) => {
			if (workflow.steps.get(name).isDefined) {
				if (!stepsTaken.isSet(name)) {
					/* Give it to first that asks - the local schedulers should be sure they can handle it */
					log.info("Giving step " + name + " to " + sender.path.address.toString)
					val step = workflow.steps(name)
					sender ! ItsYours(name, wfID, wfTempDir, wfLogDir, workflow.tokens, step)
					stepsTaken(name) = schedID
				} else {
					/* Step already taken - tell sender it's not getting the step */
					log.info("Rejecting request for step " + name + " from " + sender.path.address.toString)
					sender ! NeverMind(name, wfID)
				}
			}
			else log.error("Received request for step name not found: " + name)
		}

		/*
		 * TakingStep - answer from remote manager that it is going to execute a workflow step.
		 *
		 * name - step name
		 * wfID - "ID of workflow
		 * ports - map of socket ports assigned to receive input data
		 */
		case TakingStep(name, wfID, ports) => {
			log.info("TakingStep for " + name + " received from " + sender.path.address.toString)

			// Following may work in akka 2.1 but for now watching remote actors does not work
			//			context.watch(sender)

			/* Get remote host name */
			val host = context.sender.path.address.host match {
				case None => "localhost"
				case Some(hostName) => hostName
			}

			/* Remember who (actor) is taking step along with socket information for inputs */
			stepActors(name) = SocketActor(sender, name, host, ports)

			/* If all the steps are taken it's time to ask for the inputs to be setup */
			if (stepActors.isAllSet) {
				/* For each step there are multiple inputs  - map StepName -> map InputFlowName -> connectionInfo */
				val inputConnectionInfo : Map[String, Map[String, DataFlow]] =
					(for (stepName <- stepNames) yield
						stepName -> (for (inputFlow <- workflow.steps(stepName).inputs) yield
						{
							val (inputName, inputData) = inputFlow
							inputName -> getDataFlowConnection(inputData.sourceData,
								DataFilePointer(stepName, inputName), logDirectPipe = true)
						}).toMap).toMap

				/* Send out to each step a list of output sockets to use to communicate to following steps */
				for (stepName <- stepNames)
					stepActors(stepName).actor ! SetupInputs(inputConnectionInfo(stepName))
			}
		}

		/*
		 * Inputs are setup for a step.  If all the steps have completed setup of inputs then go ask for outputs to
		 * be setup next.
		 *
		 * name - step name
		 */
		case InputsAreSetup(name) => {

			log.info("InputsAreSetup for " + name + " received from " + sender.path.address.toString)

			/* Remember which steps have setup inputs */
			stepsInputs.set(name)

			/* If all inputs setup time to ask for the output to be setup*/
			if (stepsInputs.isAllSet) {
				/* For each step there are multiple outputs which can go to multiple places */
				val outputConnectionInfo : Map[String, Map[String, Array[DataFlow]]] =
					(for (stepName <- stepNames) yield
						stepName -> (for (outputName <- workflow.steps(stepName).outputs.keys) yield
							outputName -> (for (inputStep <- outputMap.getOrElse(DataFilePointer(stepName, outputName),
								Set.empty[String])) yield
								getDataFlowConnection(DataFilePointer(stepName, outputName),
									DataFilePointer(inputStep, outputName))).toArray).toMap).toMap
				/* Ask each step to setup it's outputs */
				for (stepName <- stepNames)
					stepActors(stepName).actor ! SetupOutputs(outputConnectionInfo(stepName))
			}
		}

		/*
		 * StepSetup - answer from remote manager that all setup is complete (e.g., sockets connected) for step.
		 *
		 * name - step name
 		 */
		case StepSetup(name) => {
			log.info("StepSetup for " + name + " received from " + sender.path.address.toString)
			stepsStarted.set(name)
			/* If all the steps are setup then tell them to start execution */
			if (stepsStarted.isAllSet) {
				log.info("Starting steps for workflow " + workflow.name + " (time=" + getTimeID(timeID) + ")")
				stepActors.values.foreach(_.actor ! StartStep)
			}
		}

		/*
		 * StepDone - answer from remote manager that step has completed.  If all the steps are done then report
		 * that the workflow is done.  Otherwise, if a step has completed with a non-success status we abort the
		 * remaining steps unless "isContinueOnError" was specified for the failing step.
		 *
		 * name - step name
		 * completionStatus - integer completion status for step
 		 */
		case StepDone(name, completionStatus) => {
			log.info("StepDone for " + name + " received from "  + sender.path.address.toString +
			  ", status: " + completionStatus.toString)
			stepsDone(name) = completionStatus
			if (stepsDone.isAllSet) {
				stepActors.values.foreach(_.actor ! CleanupStep)
				val completionStatus =
					stepActors.keys.foldLeft(0)((composite, stepName) =>
						if (composite == 0) stepsDone(stepName) else
							math.max(math.abs(composite), math.abs(stepsDone(stepName))))
				replyTo ! WorkflowDone(self.path.name, completionStatus)
				context.stop(self)
			} else if (completionStatus != 0) {
				if (!workflow.steps(name).isContinueOnError)
					self ! AbortWorkflow
			}
		}

		/*
		 * Abort the workflow - send a message to all the steps to abort
 		 */
		case AbortWorkflow => {
			log.info("Aborting workflow")
			stepActors.values.foreach(_.actor ! AbortStep)
		}

		/*
		 * Log terminated actor
 		 */
		case Terminated(deadOne) => log.info("\"" + deadOne.toString() + "\" terminated")

		/*
		 * What is the state of the workflow.  We send back the state of each step.
 		 */
		case WhatIsWorkflowState(replyTo: ActorRef) => replyTo ! WorkflowState(RUNNING,
			stepNames.foldLeft(List.empty[StepState])((listSoFar, stepName) => {
				val state = if (!stepsTaken.isSet(stepName)) NOT_TAKEN else {
					if (!stepsStarted.isSet(stepName)) NOT_STARTED else {
						if (stepsDone.isSet(stepName)) COMPLETED else RUNNING
					}
				}
				val host = if (stepActors.isSet(stepName)) Some(stepActors(stepName).host) else None
				val schedID = if (stepsTaken.isSet(stepName)) Some(stepsTaken(stepName)) else None

				val completion = if (state == COMPLETED) Some(stepsDone(stepName)) else None
				StepState(stepName,	state, schedID, host, completion) :: listSoFar
			}))

		/*
		 * Pause an output flow - we find the step controlling the flow and pass on the message
		 *
		 * flow - name of flow to pause
		 * step - name of step flow is being input into
 		 */
		case PauseSourceFlow(flow: String, step: String) => changeSourceFlowFlow("pause", flow, step, PauseFlow(flow))

		/*
		 * Resume an output flow - we find the step controlling the flow and pass on the message
		 *
		 * flow - name of flow to resume
		 * step - name of step flow is being input into
 		 */
		case ResumeSourceFlow(flow: String, step: String) => changeSourceFlowFlow("resume", flow, step, ResumeFlow(flow))

		/*
		 * Unknown message - simple log it
		 */
		case e => log.warning("received unknown message: " + e)
	}

}
