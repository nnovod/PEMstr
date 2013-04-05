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

package org.broadinstitute.PEMstr.submitXML

import akka.actor._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import org.broadinstitute.PEMstr.submitXML.xml.{WorkflowXMLPath, WorkflowXMLToDefinition, WorkflowXML}
import org.broadinstitute.PEMstr.common.CentralSchedulerMessages.StartWorkflow
import org.broadinstitute.PEMstr.common.CentralSchedulerLocations.{PEMactor, PEMactorSystem, PEMport}

/**
 * Application to submit xml files, describing workflows, to the central scheduler for execution.
 * @author Nathaniel Novod
 */
object SubmitXMLApp extends App {

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

	if (args.length != 0) println("Options found on command line: " + args.mkString(" "))
	// Go parse the command
	private val optionsFound = new SubmitXMLAppOptions(args)
	// If there was an error parsing the command line then report the error, output the correct syntax and exit
	if (!optionsFound.reportError()) {
		val centralActor = PEMactorSystem + "@" + optionsFound.getPEMhost.getOrElse(hostName) + ":" +
		  optionsFound.getPEMport.getOrElse(PEMport).toString + "/user/" + PEMactor
		/* Read in test XML and print out results */
		val workflowXML = new WorkflowXML(optionsFound.getXMLFile.get)
		  with WorkflowXMLToDefinition with WorkflowXMLPath
		println(workflowXML)
		println("\nWorkflow paths:")
		workflowXML.getAllPaths.foreach((p) => {
			println(workflowXML.drawAllPaths(p))
		})
		/* Startup akka system using config to allow remote connections */
		val akkaSystem = ActorSystem("PEMStartup", config) // , ConfigFactory.load.getConfig("remotecreation"))
		/* Now get address of remote actor to send workflow execution requests to. */
		val centralService = akkaSystem.actorFor("akka://" + centralActor)
		/* Get XML definition to submit to scheduler */
        val workflowDef = workflowXML.workflowDefinition
		/* Either retry workflow every x seconds or execute it just once */
		val retry = optionsFound.getRetry
		import akkaSystem.dispatcher
		if (retry.isDefined) {
			akkaSystem.scheduler.schedule(0 milliseconds, retry.get*1 second) {
				centralService ! StartWorkflow(workflowDef)
			}
		} else {
			centralService ! StartWorkflow(workflowDef)
			/* Give it some time to send the message before shutting down */
			akkaSystem.scheduler.scheduleOnce(3 seconds) {
				akkaSystem.shutdown()
				scala.sys.exit()
			}
		}
	}

/* Just fooling around - print out first 1000 primes
	println(prime(1000))
	def prime(wanted: Int, found: List[Int] = List.empty[Int], next: Int = 2) : List[Int] = {
		if (wanted <= found.length) found else {
			if (found.exists((i) => (next % i == 0)))
				prime(wanted, found, next+1) else
				prime(wanted, found ++ List(next), next+1)
		}
	}
*/

}
