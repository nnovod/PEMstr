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

package org.broadinstitute.PEMstr.common.workflow

import akka.actor.ActorRef
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.{StepDefinition, StepResources}

/**
 * @author Nathaniel Novod
 * Date: 12/26/12
 * Time: 8:08 PM
 *
 * Includes case classes/objects for messages sent to startup a workflow.  Note that all messages must be serializable
 * so beware if you're tempted to switch them to non-case types.
 *
 * The protocol for starting steps is:
 *
 * NewStep is sent out to subscribers wanting to execute a new step
 *
 * WantStep is returned by a remote actor that received NewStep if it decides it can execute the step.
 * The WantStep message is returned to the WorkflowManager assigned to scheduling the steps of the workflow.
 * Note that the decision about whether or not an actor can handle a step is entirely up to the actor that will
 * be handling the step.  This distributes the knowledge of who is busy to the individual actors/nodes themselves.
 *
 * ItsYours is optionally sent back by the workflow manager actor that receives a WantStep message.  It can also
 * send back a NeverMind message (e.g., if the step has already been assigned).  If an ItsYours message is sent
 * it is assumed that the remote node that receives the message will take and complete the step unless it dies.
 *
 * TakingStep is returned by a remote StepManager assigned to the step, in response to the ItYours message.
 * The TakingStep message includes the port #s and input pipes on which the step will receive incoming data.
 *
 * SetupInputs messages are sent to all the StepManagers by the WorkflowManager once all the steps
 * have responded with TakingStep.  Using the input connection information from the TakingStep messages, the
 * SetupInputs message tells each StepManager what sockets/pipes to use for receiving data from previous steps.
 *
 * InputsAreSetup is returned by a remote StepManager in response to SetupInputs once all input sockets have
 * been initialized as server sockets listening for connections.
 *
 * SetupOutputs messages are sent to all the StepManagers by the WorkflowManager once all the steps have
 * responded with InputsAreSetup.  SetupOutputs tells each StepManager what sockets/pipes to use for sending data
 * to steps that follow.
 *
 * StepSetup is returned by each StepManager once it has completed setting up the outputs needed to startup a step.
 * In particular, the client output sockets setup in response to the SetupOutputs messages must be connected to the
 * server sockets setup in response to SetupInputs messages.
 *
 * StartStep is sent out to each StepManager after all StepSetup messages have been received.  StartStep tells the
 * StepManager to start execution of the step.
 *
 * StepDone is returned by each StepManager once it has completed execution of the step.
 *
 * CleanupStep is sent out to each StepManager after all StepDone messages have been received.
 *
 */
object WorkflowMessages {

	/**
	 * Unique ID for workflow
	 *
	 * @param name workflow name
	 * @param timeID time workflow started
	 */
	case class WorkflowID(name: String, timeID: Long)

	/**
	 *
	 * First message sent - message to say that a new step is available for execution
	 *
	 * @param stepName name of step
	 * @param workflowID unique identification data for workflow
	 * @param resources resources requested for this step
	 * @param attempt # of attempts already made to get a taker for this step
	 * @param respondTo reference to WorkflowManager actor handling scheduling
	 */
	case class NewStep(stepName: String, workflowID: WorkflowID, resources: StepResources,
	                   attempt: Int, respondTo: ActorRef)

	/**
	 * First message received - message to say that node wants to take step
	 *
	 * @param stepName - name of step scheduler wants to execute
	 * @param workflowID unique identification data for workflow
	 * @param schedulerID ID for scheduler that wants step
	 */
	case class WantStep(stepName: String, workflowID: WorkflowID, schedulerID: String)

	/**
	 * Message returned to remote scheduler to say that wanted step is already taken.
	 *
	 * @param stepName - name of step scheduler wants to execute
	 * @param workflowID unique identification data for workflow
	 */
	case class NeverMind(stepName: String, workflowID: WorkflowID)

	/**
	 * Message sent to say that the step has been assigned to the receiving actor.  Note that token substitution
	 * in commands must be done by the receiving actor.
	 *
	 * @param stepName name of step scheduler wants to execute
	 * @param workflowID unique identification data for workflow
	 * @param tempDir root temporary directory to be used for pipes etc.
	 * @param logDir root temporary directory to be used for pipes etc.
	 * @param tokens map of token names to token values
	 * @param stepDef step definition
	 */
	case class ItsYours(stepName: String, workflowID: WorkflowID, tempDir: String,
	                    logDir: String, tokens: Map[String, String], stepDef: StepDefinition)

	/**
	 * Send as part of TakingStep message below.  Contains information for input to step.  The WorkflowManager, based
	 * on this information and where the other steps are executing, determines exactly how these inputs (outputs of
	 * other steps) are sent between the steps.  For steps on different nodes sockets must be used but if the output
	 * and input steps are on the same node and the output is only going to a single input then direct pipe
	 * communication can be used.
	 *
	 * @param port socket port # that can be used
	 * @param pipe specification for input pipe to be used (either via socket data or directly from output)
 	 */
	case class InputConnectionInfo(port: Int, pipe: String)

	/**
	 * Message sent by step manager, in response to ItsYours, to say it will take step
	 *
	 * @param stepName name of step to be executed
	 * @param workflowID unique identification data for workflow
	 * @param inputConnections map of connection information for inputs (keyed by flow names)
	 */
	case class TakingStep(stepName: String, workflowID: WorkflowID, inputConnections: Map[String, InputConnectionInfo])

	/**
	 * Sent as part of DataFlowSocket message below.  Contains socket connection information.  The socket server
	 * is initially setup on the input (target) side of the connection.  Then a client on the output (source) side
	 * connects to the socket to send data.
	 *
	 * @param host socket host name
	 * @param port socket port number
	 */
	case class SocketConnectionInfo(host: String, port: Int)

	/**
	 * Super class for data flow sockets and pipes.
	 */
	sealed abstract class DataFlow {
		/**
		 * Name of flow
		 */
		val flow: String
		/**
		 * Is it a pipe or a socket?
		 */
		val isPipe: Boolean
	}

	/**
	 * Data flow that uses a socket between steps
	 *
 	 * @param flowName name of flow
	 * @param socket socket connection information (host/port)
	 * @param sourceActor reference to actor managing step that is the source for the data flow
	 */
	case class DataFlowSocket(private val flowName: String, socket: SocketConnectionInfo,
	                          sourceActor: ActorRef) extends DataFlow {
		val flow = flowName
		val isPipe = false
	}

	/**
	 * Data flow that uses a pipe between steps
	 *
 	 * @param flowName name of flow
	 * @param pipe file specification of pipe to be used
	 */
	case class DataFlowPipe(private val flowName: String, pipe: String) extends DataFlow {
		val flow = flowName
		val isPipe = true
	}

	/**
	 * Message sent to give input connection information for each input file in a step.
	 *
	 * @param inputs Connections to use for inputs
	 */
	case class SetupInputs(inputs: Map[String, DataFlow])

	/**
	 * Message sent by step managers to say input sockets are ready and listening for connections.
	 *
	 * @param stepName name of step setup
	 */
	case class InputsAreSetup(stepName: String)

	/**
	 * Message sent to give output connection information for each output file in a step.
	 *
	 * @param outputs Connections to use for outputs (note an output can be sent to multiple steps)
	 */
	case class SetupOutputs(outputs: Map[String, Array[DataFlow]])

	/**
	 * Message sent by step manager when it is all setup (minimally any sockets needed have been connected to)
	 * to start executing.
	 *
	 * @param stepName name of step to be executed
	 */
	case class StepSetup(stepName: String)

	/**
	 * Message sent by workflow manager to say step can be started (sent out once all steps say they are setup).
	 * Note that it's important this be a case object and not simply a plain object to insure proper serialization.
	 */
	case object StartStep

	/**
	 * Message sent by workflow manager to say step should be aborted.
	 */
	case object AbortStep

	/**
	 * Message sent by step manager to say that the step has completed.
	 *
	 * @param stepName name of step executed
	 * @param status completion status
	 */
	case class StepDone(stepName: String, status: Int)

	/**
	 * Message sent by workflow manager to say step should be aborted.
	 */
	case object CleanupStep
	/**
	 * Abort the workflow
	 */
	case object AbortWorkflow

	/**
	 * Message sent by workflow manager to say that the workflow has completed.
	 *
	 * @param workflowName name of step executed
	 * @param status completion status
	 */
	case class WorkflowDone(workflowName: String, status: Int)

	/**
	 * States of execution
	 */
	object ExecutionState extends Enumeration {
		val NOT_TAKEN, NOT_STARTED, RUNNING, COMPLETED, UNKNOWN = Value
	}

	/**
	 * Information about the state of a step.
	 *
	 * @param name step name
	 * @param status current status
	 * @param schedID ID of scheduler assigned step
	 * @param node if running then node step is running on
	 * @param completion if completion then completion status
	 */
	case class StepState(name: String, status: ExecutionState.Value, schedID: Option[String],
	                     node: Option[String], completion: Option[Int])

	/**
	 * Message sent back about the workflow state
	 *
	 * @param status current state of workflow
	 * @param steps state of steps
	 */
	case class WorkflowState(status: ExecutionState.Value, steps: List[StepState])

	/**
	 * Message to sent to query about workflow state
	 *
	 * replyTo: send reply to this actor
	 */
	case class WhatIsWorkflowState(replyTo: ActorRef)

	/**
	 * Marker for flow control messages
	 */
	sealed trait FlowFlow

	/**
	 * Message sent to pause a flow.  This message is sent to the StepManager managing the step that is the source
	 * for the flow.
	 *
	 * @param flow name of flow
	 */
	case class PauseFlow(flow: String) extends FlowFlow

	/**
	 * Message sent to resume a paused flow.  This message is sent to the StepManager managing the step that is the
	 * source for the flow.
	 *
	 * @param flow name of flow
	 */
	case class ResumeFlow(flow: String) extends FlowFlow

	/**
	 * Message sent to pause a flow.  This message is sent to the workflow manager (most likely from a StepManager
	 * that is being overwhelmed by input from the flow) which in turn sends a message to the StepManager that
	 * controls the output of the flow.
	 *
	 * @param flow name of flow
	 * @param step name of step requesting pause
	 */
	case class PauseSourceFlow(flow: String, step: String) extends FlowFlow

	/**
	 * Message sent to resume a paused flow.  This message is sent to the workflow manager (most likely from a
	 * StepManager that has previously paused the flow) which in turn sends a message to the StepManager that
	 * controls the output of the flow.
	 *
	 * @param flow name of flow
	 * @param step name of step requesting resume
	 */
	case class ResumeSourceFlow(flow: String, step: String) extends FlowFlow

}
