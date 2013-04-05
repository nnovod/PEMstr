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
import org.broadinstitute.PEMstr.common.CentralSchedulerLocations.{PEMactor, PEMactorSystem, PEMport}
import com.typesafe.config.ConfigFactory

/**
 * @author Nathaniel Novod
 * Date: 12/19/12
 * Time: 4:03 PM
 *
 * Central scheduler for workflow manager.  We startup a akka system and then startup a single actor that has two
 * function:
 * - It receives messages asking that workflows be executed
 * - It receives subscriptions of local schedulers that want to know when workflow steps are available to be executed
 */
object CentralSchedulerApp extends App {
	private val host = java.net.InetAddress.getLocalHost.getHostName
	// Go parse the command
	private val optionsFound = new CentralSchedulerAppOptions(args)
	private val port = optionsFound.getPEMport.getOrElse(PEMport)
	/**
	 * Config to allow remote actors - this should eventually go in application.conf in the class path
	 */
	private val config = ConfigFactory.parseString("""
   akka {
     actor {
       provider = "akka.remote.RemoteActorRefProvider"
     }
     remote {
       transport = "akka.remote.netty.NettyRemoteTransport"
       netty {
         hostname = """ + 	"\"" + host + "\"" + """
         port = """ + port.toString + """
    }
  }
   }""")

	/**
	 * Startup akka system using config to allow remote connections
	 */
	private val akkaSystem = ActorSystem(PEMactorSystem, config) // , ConfigFactory.load.getConfig("remotecreation"))
	private val logger = akkaSystem.log
	logger.info("Starting central scheduler on " + host + " port " + port.toString +
	  " with command line options: " + args.mkString(" "))
	// If there was an error parsing the command line the error is reported and we exit, otherwise go on
	if (!optionsFound.reportError()) {
		/**
		 * Now startup subscriber that can receive and publish execution requests.
		 */
		akkaSystem.actorOf(Props[CentralScheduler], name = PEMactor)
	} else akkaSystem.shutdown()
}
