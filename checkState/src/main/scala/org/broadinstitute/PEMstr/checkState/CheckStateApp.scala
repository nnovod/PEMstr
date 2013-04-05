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

package org.broadinstitute.PEMstr.checkState

import akka.actor._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import org.broadinstitute.PEMstr.common.CentralSchedulerLocations._
import org.broadinstitute.PEMstr.common.CentralSchedulerMessages.{WorkflowAbort, WorkflowStatus, ListWorkflows, WorkflowList}
import scala.concurrent.{Await, Future}
import akka.pattern.{AskTimeoutException, ask}
import org.broadinstitute.PEMstr.common.workflow.WorkflowMessages.WorkflowState
import akka.util.Timeout

/**
 * @author Nathaniel Novod
 * Date: 12/30/12
 * Time: 10:35 PM
 *
 * Application to check on the state of active workflows.  Workflows can also be aborted.  Requests are sent via akka
 * to the central scheduler, which may be remote.
 */
object CheckStateApp extends App {
	/**
	 * Local host name
	 */
	private val hostName = java.net.InetAddress.getLocalHost.getHostName

	/**
	 * Config to allow remote actors
	 * This should eventually go in application.conf in the class path
 	 */
	private val config = ConfigFactory.parseString("""
    akka {
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        transport = "akka.remote.netty.NettyRemoteTransport"
        netty {
          hostname = """ + "\"" + hostName + "\"" + """
          port = 0
        }
      }
    }
	""")

	/**
	 * Amount of time to wait for a response to a request
 	 */
	private val waitTime : FiniteDuration = 3.seconds

	/**
	 * Say what options we found on the command line and go parse the options
	 */
	if (args.length != 0) println("Options found on command line: " + args.mkString(" "))
	private val optionsFound = new CheckStateAppOptions(args)

	/**
	 * Set actor specification for central scheduler to query for workflow status
	 */
	private val centralActor = PEMactorSystem + "@" + optionsFound.getPEMhost.getOrElse(hostName) + ":" +
	  optionsFound.getPEMport.getOrElse(PEMport).toString + "/user/" + PEMactor

	/**
	 * Startup akka system using config to allow remote connections and then get actor reference for central scheduler.
	 */
	private lazy val akkaSystem = ActorSystem("PEMCheck", config) // , ConfigFactory.load.getConfig("remotecreation"))
	private lazy val centralService = akkaSystem.actorFor("akka://" + centralActor)

	// If there was an error parsing the command line then report the error, output the correct syntax and exit
	if (!optionsFound.reportError()) {

		implicit val timeout = Timeout(waitTime)

		/**
		 * Get the status of a specific workflow
		 *
 		 * @param wfName name of workflow
		 */
		def dumpWorkflowStatus(wfName: String) {
			val wfFuture: Future[WorkflowState] =
				ask(centralService, WorkflowStatus(wfName)).mapTo[WorkflowState]
			val wfResult = Await.result(wfFuture, waitTime)
			println("Workflow " + wfName + " status " + wfResult.status)
			wfResult.steps.foreach((step) => {
				println("\tstep " + step.name + " " + step.status +
					" on scheduler " + step.schedID.getOrElse("<UNKNOWN>") +
					" on node " + step.node.getOrElse("<UNKNOWN>") + " with status " +
					(if (step.completion.isDefined) step.completion.get.toString else "<UNKNOWN>"))
			})
		}

		/**
		 * Process the requests
		 */
		try {
			/* See if all workflow names and/or status is wanted */
			if (optionsFound.isNamesDump || optionsFound.isStatusDump) {
				val future : Future[WorkflowList] = ask(centralService, ListWorkflows).mapTo[WorkflowList]
				val result = Await.result(future, waitTime)
				if (result.workflows.isEmpty) println("No active workflows found")
				for (wf <- result.workflows) {
					if (optionsFound.isStatusDump) {
						dumpWorkflowStatus(wf)
					} else {
						println("Workflow " + wf)
					}
				}
			}
			/* See if the status of a specific workflow is wanted */
			if (optionsFound.getWorkflowStatus.isDefined) {
				dumpWorkflowStatus(optionsFound.getWorkflowStatus.get)
			}
			/* See if there's a request to abort a workflow */
			if (optionsFound.getWorkflowAbort.isDefined) {
				val workflowName = optionsFound.getWorkflowAbort.get
				centralService ! WorkflowAbort(workflowName)
				/* Give it some time to send and process the message */
				Thread.sleep(3000)
				println("Aborted " + workflowName)
				/* Show the results */
				dumpWorkflowStatus(workflowName)
			}
		} catch {
			case e: AskTimeoutException => println("Timeout waiting for workflow state")
			case e: Exception => println("Error: " + e.getMessage)
			case _: Throwable =>
		} finally {
			/* Always shutdown akka and exit */
			akkaSystem.shutdown()
			scala.sys.exit()
		}
	}
}
