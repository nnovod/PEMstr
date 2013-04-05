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
import com.typesafe.config.ConfigFactory
import org.broadinstitute.PEMstr.common.CentralSchedulerMessages.Subscribe
import org.broadinstitute.PEMstr.common.CentralSchedulerLocations.{PEMactor, PEMactorSystem, PEMport}
import org.broadinstitute.PEMstr.common.util.Util.getTimeID

/**
 * @author Nathaniel Novod
 * Date: 8/21/12
 * Time: 10:46 AM
 *
 * Application to start a local scheduler.  A local scheduler receives requests to execute steps on the local node
 * and then takes on execution of the steps it is assigned.
 */

object LocalSchedulerApp extends App {
	/**
	 * Config to allow remote actors this should eventually go in application.conf in the class path
 	 */
	private val hostName = java.net.InetAddress.getLocalHost.getHostName
	private val config = ConfigFactory.parseString("""
    akka {
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
      }
      remote {
        transport = "akka.remote.netty.NettyRemoteTransport"
        netty {
          hostname = """ + 	"\"" + hostName + "\"" + """
          port = 0
        }
      }
    }
	""")

	/**
	 * Startup akka system using config to allow remote connections
	 */
	private val akkaSystem = ActorSystem("PEMLocal", config) // , ConfigFactory.load.getConfig("remotecreation"))
	/**
	 * Now get address of remote actor to send subscription to for new job requests and
	 * startup local scheduler actor that will receive requests.
	 */
	private val logger = akkaSystem.log
	logger.info("Starting local scheduler on host " + hostName + " with command line options: " + args.mkString(" "))
	// Go parse the command
	private val optionsFound = new LocalSchedulerAppOptions(args)
	// If there was an error parsing the command line then exit, otherwise go on
	if (!optionsFound.reportError(logger.error)) {
		// Get actorRef to central scheduler
		val centralSubscriptionService =
			akkaSystem.actorFor("akka://" + PEMactorSystem + "@" +
				optionsFound.getPEMhost.getOrElse(hostName) + ":" +
				optionsFound.getPEMport.getOrElse(PEMport).toString + "/user/" + PEMactor)
		// Create a actor for us, a local scheduler
		val localScheduler = akkaSystem.actorOf(Props(new LocalScheduler(optionsFound.getCPUs.getOrElse(4),
			optionsFound.getGigs.getOrElse(8),
			optionsFound.getID.getOrElse(hostName + "_" + getTimeID()))), name = "ExecutionNode")
		// Subscribe to get requests for steps looking for places to execute
		centralSubscriptionService ! Subscribe(localScheduler)
	} else akkaSystem.shutdown()
}