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

import workflow.WorkflowDefinitions.WorkflowDefinition
import akka.actor.ActorRef

/**
 * @author Nathaniel Novod
 * Date: 12/26/12
 * Time: 8:35 PM
 *
 * Messages sent from the central scheduler.
 */
object CentralSchedulerMessages {

	/**
	 * Message sent out to say we need to startup a new workflow.
	 *
	 * @param wf workflow definition
	 */
	case class StartWorkflow(wf: WorkflowDefinition)

	/**
	 * Message sent to request a new subscription for the availability of new steps for execution
	 *
	 * @param subscriber actor ref of subscriber (note it can be remote)
	 */
	case class Subscribe(subscriber: ActorRef)

	/**
	 * Message to get list of active workflows
 	 */
	case object ListWorkflows

	/**
	 * Message sent back with list of active workflows
	 *
 	 * @param workflows names of active workflows
	 */
	case class WorkflowList(workflows: Set[String])

	/**
	 * Message sent to query about workflow status
	 * @param name workflow to get status for
	 */
	case class WorkflowStatus(name: String)

	/**
	 * Message sent to abort a workflow
	 * @param name workflow to abort
	 */
	case class WorkflowAbort(name: String)
}
