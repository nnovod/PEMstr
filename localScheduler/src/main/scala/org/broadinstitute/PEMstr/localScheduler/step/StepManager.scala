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

package org.broadinstitute.PEMstr.localScheduler.step

import pipe.StepOutputPipe
import org.broadinstitute.PEMstr.localScheduler.LocalScheduler
import LocalScheduler.YourStep
import org.broadinstitute.PEMstr.localScheduler.step.StepManager.{ResumeSource,PauseSource,ProcessDone}
import pipe.StepOutputPipe.{OutputPipeFlow, ResumePipe, PausePipe}
import socket.{StepInputSocketRewind,StepInputSocket,StepOutputSocket,SocketDataEventBus}
import socket.StepInputSocket.{SocketListening, FirstSocketData}
import org.broadinstitute.PEMstr.common._
import util.{StartTracker, ValueTracker}
import StepCommandArgTokens.{getVariableValue,replaceStreamsAndVariables,replaceVariables}
import workflow.WorkflowDefinitions._
import workflow.WorkflowMessages._
import scala.Some
import scala.sys.process._
import akka.actor._
import scala.concurrent.Future
import java.io.{FileWriter,FileNotFoundException,File}
import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel
import scala.util.{Failure, Success}
import org.broadinstitute.PEMstr.common.ActorWithContext.MutableContext
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import scala.concurrent.duration._


/**
 * @author Nathaniel Novod
 * Date: 8/16/12
 * Time: 2:05 PM
 *
 * Top level actor to manage the execution of a step.  Life really starts when it receives a YourStep message
 * specifying the particulars of a step to be executed.
 */
class StepManager extends ActorWithContext {

	/**
	 * A simple strategy for now - abort child on exception and then abort the step
 	 */
	override val supervisorStrategy =
		OneForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1.minute) {
			case _: Exception ⇒ {
				self ! AbortStep
				Stop
			}
		}

	/**
	 * Used to track state of input data used for step execution.  Note that access to this class and other items
	 * following is a bit strange (protected to ourselves) because it must be seen within LocalContext which must be
	 * used for getMyContext which is an override of a protected method in a super class.  Ideally the use of
	 * protected[StepManager] could be replaced by a simple private but that's not possible.
	 *
 	 * @param port socket port # used (valid if and only if the socket is open)
	 * @param socket socket channel used (valid if and only if the socket is open)
	 * @param listening tracks once we're listening on the socket
	 * @param connection tracks how input connection has been made
	 */
	protected[StepManager] case class InputData(port: Int, socket: ServerSocketChannel, listening: StartTracker,
	                                            connection: ValueTracker[DataFlow])

	/**
	 * Used to track state of output data used for step execution
	 *
 	 * @param flows outputs for step
	 * @param pipeActor actor used for output pipe
	 */
	protected[StepManager] case class OutputData(flows: Array[DataFlow], pipeActor: ValueTracker[ActorRef])

	/**
	 * Actor context - it is saved in instance between invocations of actor and also restored to any new instances
	 * of the actor that get created due to restarts.  value "locals" is used to refer to context.
	 */
	protected[StepManager] class LocalContext extends MutableContext {
		/* Saved parameters about step to be executed */
		val stepParams = new ValueTracker[YourStep]
		/* Saved input settings */
		val inputData = new ValueTracker[Map[String, InputData]]
		/* Saved output settings */
		val outputData = new ValueTracker[Map[String, OutputData]]
		/* Where to send back message if setup completed (should be remote WorkflowManager) */
		val actorToReplyToWhenSetup = new ValueTracker[ActorRef]
		/* Where to send back status messages to (should also be remote WorkflowManager) */
		val actorToTrackWorkflowStatus = new ValueTracker[ActorRef]
		/* Other place to send back message when we're done (should be LocalScheduler) */
		val actorWhoGaveUsStep = new ValueTracker[ActorRef]
		/* Flag to indicate that the process to be executed for this step has been started */
		val processStarted = new StartTracker
		/* Status to indicate that the process to be executed for this step has been started */
		val processCompleted = new ValueTracker[Int]
		/* Process running for step */
		val process = new ValueTracker[Process]
		/* We're done - just waiting to cleanup */
		val done = new StartTracker
		/* Temp directory used */
		val tempDirCreated = new ValueTracker[String]
		/* Log directory used */
		val logDirCreated = new ValueTracker[String]
	}

	/**
	 * What our context looks like
 	 */
	override protected[StepManager] type MyContext = LocalContext

	/**
	 * Create a new context.
	 *
	 * @return new instance of local context
	 */
	override protected[StepManager] def getMyContext = new LocalContext

	/**
	 * Check if the step is all done.  If the associated process has completed and all children are done then we're
	 * done as well.
	 */
	private def isStepDone = locals.processCompleted.isSet && context.children.forall(_.isTerminated)

	/**
	 * Check if the step is all done.  If the associated process has completed and all children are done then we're
	 * done as well.
	 */
	private def shutdownChildren() {
		context.children.foreach(context.stop(_))
	}

	/**
	 * Shut down the actor. Send a message back saying we're done to interested parties.
 	 */
	private def weAreDone() {
		if (!locals.done.isSet) { // Make sure we only send out the messages once
			val doneVal = if (locals.processCompleted.isSet) locals.processCompleted() else 1
			val doneMsg = StepDone(locals.stepParams().stepName, doneVal)
			if (locals.actorToTrackWorkflowStatus.isSet) locals.actorToTrackWorkflowStatus() ! doneMsg
			locals.actorWhoGaveUsStep() ! doneMsg
			locals.done.set()
		}
	}

	/**
	 * Need to make sure all input sockets are listening before we declare ourselves ready to go.  Otherwise, if there
	 * is little data sent to the socket, it could be closed by the source before we even start listening.
	 *
	 * @return true if all the input are either listening on a socket or are not using a socket
	 */
	private def isEveryoneListening =
		locals.inputData().values.forall((data) => data.listening.isSet || !data.socket.isOpen)

	/**
	 * Get an iterator over the input connections
	 *
 	 * @return iterable collection of input data flow information
	 */
	private def getInputConnections = locals.inputData().values.map(_.connection())

	/**
	 * Init all the input streams with port/socket information and place to set other values later.
	 *
 	 * @param step definition for step including inputs
	 *
	 * @return map with an entry per input stream (note join is multiple streams)
	 */
	private def initInputData(step: StepDefinition) = {
		/**
		 * Get a new InputData entry with a newly allocated port and socket.
		 *
		 * @return input data entry to track input setup information initialized with port and socket
		 */
		def getInputDataEntry = {
			val (socket, port) = getSocket
			InputData(port, socket, new StartTracker, new ValueTracker[DataFlow])
		}

		/**
		 * Get map to track data of input streams.  It's initialized here with port and serversocket information.
 		 */
		step.inputs.map(_._1 -> getInputDataEntry)
	}

	/**
	 * Find the input data entry associated with a port.
	 *
	 * @param portNumber port # to search for
	 *
	 * @return optional input data found
	 */
	private def findInputData(portNumber: Int) =
		locals.inputData().values.find(_.port == portNumber)

	/**
	 * Method to startup the process and send a message when it completes.  The process is run in
	 * a separate thread so that the actor will not need to pend waiting for completion.  Upon completion
	 * a message is sent back to the actor declaring the step done including the completion code.
	 *
	 */
	private def runStepProcess() {
		/* If the process has already been started then there's nothing to do */
		if (!locals.processStarted.isSet  && !locals.done.isSet) {
			/* Flag that the process is being started */
			locals.processStarted.set()
			/* Retrieve settings for step - it must have already been setup before we get here */
			val stepSettings = locals.stepParams()

			/**
			 * Open a file writer for logging
			 *
			 * @param fileExtension file extension
			 *
			 * @return open FileWriter and associated File
			 */
			def logFile(fileExtension: String) = {
				val file = new File(locals.logDirCreated() + "/" + stepSettings.stepName + fileExtension)
				(new FileWriter(file), file)
			}
			/**
			 * Cleanup a log file: close the file and delete it if there's nothing in there.
			 *
			 * @param file tuple with FileWriter and File for log file
			 */
			def cleanupLogFile(file: (FileWriter, File)) {
				file._1.close()
				if (file._2.length == 0) file._2.delete()
			}

			/**
			 * Build a process to execute combining all the commands specified for the step
			 *
 			 * @param commands commands to be executed for this step
			 * @return process builder for all the commands
			 */
			def buildProcess(commands: Seq[StepCommand]) : ProcessBuilder = {
				val proc = buildSingleProcess(commands.head)
				if (commands.tail.isEmpty) proc else {
					/* Recurse to get rest of commands */
					val rest = buildProcess(commands.tail)
					/* and say whether to continue (###) or not (#&&) on to other commands when there is an error */
					if (commands.head.isContinueOnError) proc ### rest else	proc #&& rest
				}
			}

			/**
			 * Build a process to execute a command.  If a working directory or environment variables are associated
			 * with the command they are passed to the process builder,
			 *
			 * @param command command to be executed
			 * @return process builder for command
			 */
			def buildSingleProcess(command: StepCommand) = {
				/* Get the process arguments replacing tokens with file specifications/token values */
				val commandArgs = command.args.flatMap(subFileName(stepSettings, _))
				/* Log what we're executing */
				log.info("Executing command: " + commandArgs.mkString(" "))
				/* Get optional working directory for ProcessBuilder */
				val file = replaceVars(stepSettings, command.workingDirectory) match {
					case Some(dir) => Some(new File(dir))
					case None => None
				}
				/* Make the ProcessBuilder - Process has apply methods to held make the ProcessBuilder */
				Process(commandArgs, file, command.envVars: _*)
			}

			/* Make a ProcessBuilder for all the commands */
			val commands = stepSettings.stepDef.commands
			if (commands.isEmpty) {
				log.warning("No commands specified - just exiting")
				self ! ProcessDone(0)
			} else {
				try {
					/* Open the log files. */
					val outLog = logFile(".out")
					val errLog = logFile(".err")
					/* Build the process */
					val processToRun = buildProcess(commands)
					/* Get a process with an associated logger */
					val processLogger = ProcessLogger(
						(output) =>	outLog._1.write(output + "\n"),
						(error) => errLog._1.write(error + "\n")
					)
					val processRunning = processToRun.run(processLogger)
					locals.process() = processRunning
					/**
					 * Now we wait for completion of the process by using a Future to run the expression that waits for
					 * completion in a separate thread.  The expression looks for the running process' exit value, so it
					 * will not complete until the process exits.  When it does complete a message will be sent to this
					 * actor with the process completion status.
					 */
					log.info("Starting process for " + stepSettings.stepName)
					import context.dispatcher
					Future {
						processRunning.exitValue()
					} onComplete {
						case Success(result) => {
							self ! ProcessDone(result)
							cleanupLogFile(outLog)
							cleanupLogFile(errLog)
						}
						case Failure(err) => {
							log.error("Process failed to complete: " + err.getMessage)
							self ! ProcessDone(1)
							cleanupLogFile(outLog)
							cleanupLogFile(errLog)
						}
					}
				} catch {
					case e: Exception => log.error("Error starting process: " + e.getMessage)
					self ! ProcessDone(1)
				}
			}
		}
	}

	/**
	 * Method called to insure that the process has been started.  This is called when any of the input sockets
	 * associated with the step publish activity.  By waiting for the sockets to receive data (or close if there's
	 * no data coming) we can delay running the process until there's actually data to work on.
 	 */
	private def delayedProcessStart() {
		log.debug("Received socket data when process " +
			(if (locals.processStarted.isSet) "is" else "not") + " started")
		runStepProcess()
	}

	/**
	 * Create a directory.  If a directory doesn't exist yet we create it.
	 *
	 * @param dir specification of directory to be created
	 *
	 * @return true if directory has been created (or already existed
 	 */
	private def mkdir(dir: String) = {
		val dirF = new File(dir)
		def dirSet = (dirF.exists() && dirF.isDirectory)
		dirSet || dirF.mkdirs() || dirSet
	}

	/**
	 * Get a server socket/port for a channel
	 *
 	 * @return (open server socket channel, local port # used for socket)
	 */
	private def getSocket = {
		val serverChannel = ServerSocketChannel.open()
		serverChannel.socket().bind(new InetSocketAddress(0))
		(serverChannel, serverChannel.socket().getLocalPort)
	}

	/**
	 * Run a command in a separate process waiting for its completion.
	 *
 	 * @param cmd command to execute
	 *
	 * @return return status from executed command
	 */
	private def runUnixCmd(cmd: String) = {
		/* Get a process with an associated logger */
		val processToRun = Process(cmd)
		val processLogger = ProcessLogger(
			(output) => log.info(cmd + " output: " + output),
			(error) => log.error(cmd + " error: " + error)
		)
		/* Start the process asynchronously */
		val processRunning = processToRun.run(processLogger)
		/* Wait for it to complete and return completion value */
		processRunning.exitValue()
	}

	/**
	 * Create a pipe.  If the file already exists we first rename it before creating a piped file.  We also make sure
	 * that the directory to contain the file exists.  It may not for "hidden" files.
	 *
	 * @param fileSpec specification for pipe to be created
 	 */
	private def mkfifo(fileSpec: String) {
		val fileF = new File(fileSpec)
		if (fileF.exists()) {
			/* File already exists - rename it to <name>.bak */
			if (fileF.length == 0) fileF.delete() else {
				log.warning("Renaming " + fileSpec + "to " + fileSpec + ".bak")
				fileF.renameTo(new File(fileSpec + ".bak"))
			}
		} else {
			/* File doesn't exists - just make sure the directory exists */
			val fileD = fileF.getParentFile
			if (fileD == null) throw new FileNotFoundException("Error creating named pipe \"" + fileSpec +
				"\": invalid filename or parent directory")
			if (!fileD.exists()) fileD.mkdirs()
		}
		/* Go create the pipe - no way to do this except to fork off a unix command */
		val status = runUnixCmd("mkfifo " + fileSpec)
		if (status != 0) throw new FileNotFoundException("Error creating named pipe \"" + fileSpec + "\": " + status)
	}

	/**
	 * Get full specification for a file in the workflow's temporary directory
	 *
	 * @param stepName name of step associated with the file
	 * @param dataName name of flow associated with the file
	 * @param extension file extension to use for file
	 * @param tempDir workflow's temporary directory in which file resides
	 *
	 * @return specification of file
	 */
	private def getFileName(stepName: String, dataName: String, extension: String, tempDir: String) =
		tempDir + "/" + stepName + "_" + dataName + "." + extension

	/**
	 * Get full specification for a step's input file in the workflow's temporary directory
	 *
	 * @param stepName name of step associated with the file
	 * @param dataName name of flow associated with the file
	 * @param tempDir workflow's temporary directory in which file resides
	 *
	 * @return specification of file
	 */
	private def getInputName(stepName: String, dataName: String, tempDir: String) =
		getFileName(stepName, dataName, "inp", tempDir)

	/**
	 * Get full specification for a step's output file in the workflow's temporary directory
	 *
	 * @param settings step information
	 * @param stepName name of step associated with the file
	 * @param dataName name of flow associated with the file
	 * @param tempDir workflow's temporary directory in which file resides
	 *
	 * @return specification of file
	 */
	private def getOutputName(settings: YourStep, stepName: String, dataName: String, tempDir: String) = {
		settings.stepDef.outputs(dataName).hiddenAs match {
			case Some(hiddenAs) => replaceVariables(hiddenAs, (tokenValue) =>
				getVariableValue(settings.stepDef, settings.tokens, settings.wfID, tokenValue))
			case None => getFileName(stepName, dataName, "out", tempDir)
		}
	}

	/**
	 * Get optional value with variable replacment
	 *
	 * @param settings step information
	 * @param str string in which to do variable replacement
 	 */
	private def replaceVars(settings: YourStep, str: Option[String]) = {
		str match {
			case Some(s) => Some(replaceVariables(s, (tokenValue) =>
				getVariableValue(settings.stepDef, settings.tokens, settings.wfID, tokenValue)))
			case None => None
		}
	}


	/**
	 * Get the name of the checkpoint file, if one is specified.
	 *
	 * @param settings step information
	 * @param dataName name of flow associated with the file
	 *
	 * @return optional specification to be used for checkpoint file
	 */
	private def getCheckpointName(settings: YourStep, dataName: String) =
		replaceVars(settings, settings.stepDef.outputs(dataName).checkpointAs)

	/**
	 * If checkpointing of the output stream is happening then we add the checkpoint actor as a consumer
	 * of the stream.
	 *
	 * @param settings step information
	 * @param dataName name of flow associated with the file
	 * @param consumers actors that will be consuming output pipes data
	 *
	 * @return consumers list including checkpoint actor if checkpointing is happening
	 */
	private def addCheckpoint(settings: YourStep, dataName: String, consumers: Seq[(String, ActorRef)]) = {
		getCheckpointName(settings, dataName) match {
			case Some(checkpointSpec) => {
				/**
				 * Create an actor to write out the checkpoint data.  The actor subscribes to the output pipe bus
				 * to know when data is to be sent over the socket.
				 */
				val actorName = "checkpoint_" + dataName
				val actor = context.actorOf(Props(
					new StepCheckpointFile(checkpointSpec)), name = actorName)
				context.watch(actor)
				consumers :+ actorName -> actor
			}
			case None => consumers
		}
	}

	/**
	 * Replace occurrences of stream tokens ({stream=<name>}) and token tokens (({token=<name>}) in a command argument
	 * with the actual stream file specifications or token values.  There can be more than one file specification
	 * if the output is being split or the input is being joined.  The arguments are returned as a list to allow
	 * multiple specifications.
	 *
	 * @param settings step settings, including input and output files
	 * @param arg argument string in which to replace tokens
	 *
	 * @return argument strings with streams and tokens replaced by file specifications and token values
	 */
	private def subFileName(settings: YourStep, arg: String) = {

		/**
		 * Look for a match for the wanted flow in a collection of flows.  If a match is found return the file
		 * specification for the match.
		 *
		 * @param name file name to search for
		 * @param flows collection of flows
		 * @param makeFileName callback to create a file name if a socket flow
		 *
		 * @return optional file specification to use for flow
		 */
		def findFileSpec(name: String, flows: Iterable[DataFlow], makeFileName: () => String) : Option[String] = {
			flows.find(_.flow == name) match {
				/* If a pipe then use the pipe specification */
				case Some(DataFlowPipe(_, pipe)) => Some(pipe)
				/* If a socket then create file specification for associated pipe */
				case Some(DataFlowSocket(_, _, _)) => Some(makeFileName())
				case None => None
			}
		}

		/**
		 * Find an input file specification for a file name (name associated with a "stream" in the original workflow
		 * definition).
		 *
		 * @param name file name to search for
		 *
		 * @return complete file specification being used
		 */
		def findInputFileName(name: String) = {
			/* Check if flow is in input connections, if not there then then we're in trouble. */
			val spec = findFileSpec(name, getInputConnections,
				() => getInputName(settings.stepName, name, settings.tempDir))
			assert(spec.isDefined, "Couldn't find input stream to substitute for \"" + name +
			  "\" in step \"" + settings.stepName + "\"")
			spec.get
		}

		/**
		 * Find an output file specification for a file name (name associated with a "stream" in the original workflow
		 * definition).
		 *
		 * @param name file name to search for
		 *
		 * @return complete file specification being used
		 */
		def findOutputFileName(name: String) = {
			/* Check if flow is in outputs. If not there then we're in trouble. */
			val spec = findFileSpec(name, locals.outputData().values.flatMap(_.flows),
				() => getOutputName(settings, settings.stepName, name, settings.tempDir))
			assert(spec.isDefined, "Couldn't find output stream to substitute for \"" + name +
			  "\" in step \"" + settings.stepName + "\"")
			spec.get
		}

		/**
		 * Get separator to put between join/split file names
		 *
 		 * @param spec specification for data we're looking at
		 *
		 * @return optional separator found
		 */
		def getSeparator(spec: AnyRef) = {
			spec match {
				case so: SplitOutputStreamFile => so.split.separator
				case o: OutputStreamFile => None
				case ji: JoinInputStreamDataFlow => ji.joinSpec.separator
				case j: InputStreamDataFlow => None
				case _ => None
			}
		}

		/**
		 * Get all the files specified of a given name into an array with any separator found.
		 *
 		 * @param input list of instances found with same base name
		 *
		 * @return array of file names and optional separator to place between names
		 */
		def makeStrArray(input: Iterable[(ItemInstance, Option[String])]) = {
			input.foldLeft((Array.empty[ItemInstance], None: Option[String]))((soFar, inp) =>
				(soFar._1 :+ inp._1, if (soFar._2.isDefined) soFar._2 else inp._2))
		}

		/* Regular expression to use to split names - we just need something that would not be in user's arguments */
		val argSplitSpec = "::ArG::"

		/* Replace ${stream=...} and ${token=...} variables with actual file specifications or token values */
		val args = replaceStreamsAndVariables(arg,
			(streamName) => {
				/*
				 * If a stream - check for an input or output stream (never matches both since names are unique)
				 * Get list of names - there will be more than one for input joins and output splits.  When
				 * more than one file is there we want to keep the files in consistent order in the command
				 * so we sort by instance number.
				 */
				val namesData : ((Array[ItemInstance], Option[String]), (String) => String) = {
					val inNames = settings.stepDef.inputs.values.flatMap((dataFlow) => {
						if (dataFlow.stream.baseName != streamName) List.empty[(ItemInstance, Option[String])] else
							List((dataFlow.stream, getSeparator(dataFlow)))
					})
					/* If we found input then we're set, otherwise we need to look for stream in output */
					if (!inNames.isEmpty) (makeStrArray(inNames), findInputFileName) else {
						val outNames = settings.stepDef.outputs.values.flatMap((output) => {
							if (output.baseName != streamName) List.empty[(ItemInstance, Option[String])] else
								List((output, getSeparator(output)))
						})
						(makeStrArray(outNames), findOutputFileName)
					}
				}
				val (names, separator) = namesData._1
				val findFileName = namesData._2
				/* Go sort by instance number */
				val sortedInstances = names.sortWith((e1, e2) => {
					/* If we get here all instance numbers should be defined but we check just in case */
					if (e1.instance.isEmpty || e2.instance.isEmpty)
						e1.instance.isEmpty else e1.instance.get < e2.instance.get
				})
				/**
				 * Now make a single string of all this with instances separated by a specified or funny separator.
				 * If a separator was specified we make a single argument of the list of names using the separator
				 * between them.  If no separator is specified then we put in the funny separator and later split
				 * the made string into multiple arguments while removing the funny separator.
				 */
				sortedInstances.map((e) => findFileName(e.name)).mkString(separator.getOrElse(argSplitSpec))
			},
			(tokenName) => getVariableValue(settings.stepDef, settings.tokens, settings.wfID, tokenName))

		/* Now split into separate arguments if separator not specified */
		args.split(argSplitSpec)
	}

	/**
	 * Output warning that wanted flow not found
	 *
 	 * @param flow name of flow
	 */
	private def flowNotFound(flow: String) { log.warning("Can't find wanted flow:" + flow) }

	/**
	 * Get source actor for a flow.
	 *
	 * @param flow name of flow
	 *
	 * @return optional reference to actor managing source step for flow
 	 */
	private def getSourceStepActor(flow: String) : Option[ActorRef]= {
		locals.inputData().get(flow) match {
			case Some(input) => if (!input.connection.isSet) None else input.connection() match {
				case DataFlowSocket(_, _, sourceActor) => {
					Some(sourceActor)
				}
				case _ => None
			}
			case None => {
				flowNotFound(flow)
				None
			}
		}

	}


	/**
	 * Change (pause or resume) flow of an output pipe.
	 *
 	 * @param flow name of flow
	 * @param msg message to send to flow actor
	 */
	private def flowChange(flow: String, msg: OutputPipeFlow) {
		locals.outputData().get(flow) match {
			case Some(output) => if (output.pipeActor.isSet) output.pipeActor() ! msg else flowNotFound(flow)
			case None => flowNotFound(flow)
		}
	}

	/**
	 * Heart of actor - receive and respond to messages
	 */
	def receive = {
		/*
		 * You asked for it, you got it: The Step is Yours to execute!  Create the necessary directory and sockets to
		 * read the input data coming from other steps.  Note that we assign a socket to each input here but if
		 * we wind up using a direct pipe for communication then the socket is freed when we see that a direct pipe
		 * is being used in the SetupInputs message.
		 *
		 * name: step name
		 * wfID: ID of workflow
		 * tempDir: workflow temporary directory
		 * logDir: logging directory
		 * stepDef: step definition
		 * tokens: tokens to be replaced in command args
		 * replyTo: reference to actor to which to send reply that we're taking on the step
		 */
		case myStep @ YourStep(name, wfID, tempDir, logDir, stepDef, tokens, replyTo) => {
			def mkLocalDir(dir: String) {
				if (!mkdir(dir)) throw new FileNotFoundException("Error creating directory " + dir)
			}
			/* Save step info - note this actor should only get one YourStep message */
			locals.stepParams() = myStep
			/* Make sure temporary directory for workflow exists locally */
			mkLocalDir(tempDir)
			locals.tempDirCreated() = tempDir
			/* Make sure log directory for workflow exists locally */
			mkLocalDir(logDir)
			locals.logDirCreated() = logDir
			/*
			 * Get a socket port for each input and remember the related server socket created
			 * This is a bit ugly since we have to get rid of the port later (see StartStep) if it's never used
			 * because the input is piped
			 */
			locals.inputData() = initInputData(stepDef)
			val inputPorts = locals.inputData().map((i) => {
				val (streamName, data) = i
				streamName -> InputConnectionInfo(data.port, getInputName(name, streamName, tempDir))
			})
			/* Respond that we're happily taking on the step */
			replyTo ! TakingStep(name, wfID, inputPorts)
			/* Save who to send message to when we're done */
			locals.actorToTrackWorkflowStatus() = replyTo
			/* Save who gave us the step (local scheduler) */
			locals.actorWhoGaveUsStep() = sender
		}

		/*
		 * Message containing how input connections should be made.
		 *
		 * inputs: map of connection information for each input (either port or pipe to use)
		 */
		case SetupInputs(inputs: Map[String, DataFlow]) => {
			try {
				/* Get saved step settings */
				val stepSettings = locals.stepParams()
				/**
				 * Input is of two types:
				 * - socket input that we receive from another step via a socket and then pipe into our step
				 * - piped input that we receive directly from an output pipe from a previous step
				 *
				 * For piped input we don't do anything except create the pipe since the data is going directly between
				 * the executing steps' processes via the pipe.  This is most efficient, however not always possible.
				 * In particular, if the two steps are executing on different nodes or the input being received is being
				 * sent by the previous step to multiple destinations then sockets must be used.
				 *
				 * For each socket input we setup a bus that can be subscribed to to know about events on the socket.
				 * Each input subscribes to the bus in order to know when any data is received so that we know when to
				 * start executing the process associated with the step.  We could start the process now but it might
				 * just hang around doing nothing if input data doesn't arrive soon.
				 *
				 * For each socket input we also setup new actors to handle the socket and pipe associated with the
				 * input.  The socket actor (see StepInputSocket) receives messages when socket activity occurs and
				 * passes the information onto subscribers (e.g., the input pipe).
				 *
				 */
				for (input <- inputs)  {
					val (iName, iData) = input
					/* Save the DataFlow associated with the input */
					locals.inputData()(iName).connection() = iData
					iData match {
						/**
						 * Input is being received over a socket  - do all the initial setup to get the socket ready
						 * to receive data and the pipe actor ready to get data from the socket to send it to the input
						 * pipe setup to the step.
						 */
						case DataFlowSocket(flow, socket, sourceActor) => {
							val bus = new SocketDataEventBus
							bus.subscribe(self, classOf[FirstSocketData])
							bus.subscribe(self, classOf[SocketListening])
							val inputPipeSpec = getInputName(stepSettings.stepName, flow, stepSettings.tempDir)
							mkfifo(inputPipeSpec)
							def getInputSocket = {
								val inputData = findInputData(socket.port).get
								val server = inputData.socket
								val inputDef = stepSettings.stepDef.inputs(iName).stream
								inputDef.rewind match {
									case Some(rewindSpec) =>
										new StepInputSocketRewind(flow, server, self, bus, rewindSpec,
											inputPipeSpec, inputDef.bufferOptions)
									case _ => new StepInputSocket(flow, server, self, bus,
										inputPipeSpec, inputDef.bufferOptions)
								}
							}
							val inputActor = context.actorOf(Props(getInputSocket),
								name = "inputSocket_" + flow + "_" + socket.port)
							context.watch(inputActor)
						}
						/* Input is received directly over a pipe - just make the pipe and close unused socket */
						case DataFlowPipe(_, pipe) => {
							mkfifo(pipe)
							locals.inputData()(iName).socket.close()
						}
					}
				}

				/* Save who to send message to when we're all setup (input sockets are ready) */
				locals.actorToReplyToWhenSetup() = sender
				/* Check if we can send out the setup message now */
				if (isEveryoneListening) sender ! InputsAreSetup(stepSettings.stepName)
			} catch {
				case fnf: FileNotFoundException => {
					log.error("Input setup failed: " + fnf.getMessage)
					self ! AbortStep
				}
				case e: Exception => {
					log.error("Input setup failed: " + e.getMessage + "\n" + e.getStackTraceString)
					self ! AbortStep
				}
			}
		}

		/*
		 * A connection to an input socket has completed - we record that another socket is all set and if all input
		 * sockets are now set we send a message back saying we're all setup to go.
		 */
		case SocketListening(port: Int) => {
			findInputData(port) match {
				case Some(data) => data.listening.set()
				case None =>
			}
			if (isEveryoneListening) locals.actorToReplyToWhenSetup() ! InputsAreSetup(locals.stepParams().stepName)
		}

		/*
		 * Message containing how output connections should be made.  Note that outputs can go to multiple places.
		 *
		 * outputs: map of connection information for each output (either array of ports or single pipe)
		 */
		case myOutputs @ SetupOutputs(outputs: Map[String, Array[DataFlow]]) => {
			try {
				/* Get saved step settings */
				val stepSettings = locals.stepParams()
				/* Save connection info */
				locals.outputData() = myOutputs.outputs.map((entry) => {
					val (name, outputs) = entry
					name -> OutputData(outputs, new ValueTracker[ActorRef])
				})
				/**
				 * Go through the outputs.  For each output that is not being directly piped to the next step we
				 * create the pipe we'll use to receive output from our step and then setup sockets that will be used
				 * to send the data on to the steps receiving the output from our step.
				 */
				for (output <- outputs.values if (output.exists(!_.isPipe))) {
					val outputName = output(0).flow
					/* Get name we'll use for pipe  and create the pipe */
					val outputFileName = getOutputName(stepSettings, stepSettings.stepName,
						outputName, stepSettings.tempDir)
					mkfifo(outputFileName)
					/**
					 * The output is directed to one or more inputs of other steps - here we go through and setup
					 * an actor to handle each output socket that will send data out to other steps' input.
					 */
					val sockets = for (flowIndex <- output.indices) yield {
						assert(output(flowIndex).isInstanceOf[DataFlowSocket], "Non-piped output is not socket")
						val dataFlow = output(flowIndex).asInstanceOf[DataFlowSocket]
						val socket = dataFlow.socket
						/**
						 * Create an actor to send the data over a socket to the following step.  The actor
						 * subscribes to the output pipe bus to know when data is to be sent over the socket.
						 */
						val socketName = "outputSocket_" + outputName + "_" + flowIndex
						val actor = context.actorOf(Props(
							new StepOutputSocket(socket.host, socket.port)), name = socketName)
						context.watch(actor)
						/* yield is building up map of socket names to actor references for the socket */
						socketName -> actor
					}
					/* Add checkpoint, if specified, as an additional consumer */
					val consumers = addCheckpoint(stepSettings, outputName, sockets)
					/* Setup the pipe actor to read data coming from the process and send it out to the sockets */
					val bufferOptions = stepSettings.stepDef.outputs(outputName).bufferOptions
					val outputPipeActor =
						context.actorOf(Props(new StepOutputPipe(consumers.toMap, outputFileName, bufferOptions)),
							name = "output_" + outputName)
					context.watch(outputPipeActor)
					locals.outputData()(outputName).pipeActor() = outputPipeActor
				}
				sender ! StepSetup(stepSettings.stepName)
			} catch {
				case fnf: FileNotFoundException => {
					log.error("Input setup failed: " + fnf.getMessage)
					self ! AbortStep
				}
				case e: Exception => {
					log.error("Output setup failed: " + e.getMessage + "\n" + e.getStackTraceString)
					self ! AbortStep
				}
			}
		}


		/*
		 * Time to startup the step process.  We start up the process immediately if one of the following is true:
		 * - There is an input pipe getting data directly for another step's output pipe
		 * - There are no inputs
		 *
		 * Otherwise we wait till some input comes to start the process later.
 		 */
		case StartStep => {
			log.info("Starting step")
			if (getInputConnections.exists((inp) => inp.isPipe) ||
				locals.stepParams().stepDef.inputs.size == 0) runStepProcess()
		}

		/*
		 * Request to abort the step - we simply abort the process and hopefully the rest falls out
		 * @TODO destroy only kills parent process - if there are children they are orphaned and continue running
		 */
		case AbortStep => {
			log.info("Abort step received")
			if (locals.process.isSet && !locals.processCompleted.isSet) locals.process().destroy() else
				if (locals.processCompleted.isSet) shutdownChildren() else self ! ProcessDone(1)
		}

		/*
		 * The process has completed.  We log the completion and then shut down any children if the process was aborted.
		 * Once the children are done we declare that we're all done.
		 *
		 * name: step name
		 * value: process completion status
		 */
		case ProcessDone(value) => {
			if (!locals.processCompleted.isSet) { // May already be complete in case of abort(s)
				log.info("Process completed with status " + value)
				locals.processCompleted() = value
			}
			if (value != 0) shutdownChildren()
			if (isStepDone) weAreDone()
		}

		/*
		 * Pause a source feeding an input for this step.  Send message back to step manager controlling source step.
		 *
		 * flow: name of flow to pause
 		 */
		case PauseSource(flow: String) => getSourceStepActor(flow) match {
			case Some(actor) => actor ! PauseFlow(flow)
			case None =>
		}
		/*
		 * Alternative way: Send message back to workflow manager which will send PauseFlow message off to source step.
		 *
		 * flow: name of flow to pause
 		 */
		//case PauseSource(flow: String) => actorToTrackWorkflowStatus() ! PauseSourceFlow(flow, stepParams().stepName)


		/*
		 * Resume a source feeding input to this step.  Send message back to step manager controlling source step.
		 *
		 * flow: name of flow to resume
 		 */
		case ResumeSource(flow: String) => getSourceStepActor(flow) match {
			case Some(actor) => actor ! ResumeFlow(flow)
			case None =>
		}
		/*
		 * Alternative way: Send message back to workflow manager which will send ResumeFlow message off to source step.
		 *
		 * flow: name of flow to resume
 		 */
		//case ResumeSource(flow: String) => actorToTrackWorkflowStatus() ! ResumeSourceFlow(flow, stepParams().stepName)

		/*
		 * Pause a source coming from this step.
		 *
		 * flow: name of flow to pause
 		 */
		case PauseFlow(flow: String) => flowChange(flow, PausePipe)

		/*
		 * Resume a source coming from this step.
		 *
		 * flow: name of flow to resume
 		 */
		case ResumeFlow(flow: String) =>  flowChange(flow, ResumePipe)

		/*
		 * A child (socket or pipe actor) has terminated
		 *
		 * child: ActorRef for child
 		 */
		case Terminated(child) => {
			log.info(child.path.name + " terminated")
			if (isStepDone)  weAreDone()
		}

		/*
		 * An input socket has received data.  This message is received via the subscription to input socket(s).
		 * If the process was waiting for input make sure it starts now.
		 *
		 * data: # of bytes received
		 */
		case FirstSocketData(length) => delayedProcessStart()

		/*
		 * Time to cleanup - workflow must have completed so now we cleanup temp directories and stop ourself.
		 */
		case CleanupStep => {
			log.info("cleanup")
			if (locals.tempDirCreated.isSet) {
				val dirF = new File(locals.tempDirCreated())
				try {
					for (file <- dirF.listFiles() if file != null) file.delete()
				} catch {
					case e: Throwable =>
				}
				dirF.delete()
			}
			context.stop(self)
		}

		/*
		 * Unknown message
		 */
		case e => log.warning("received unknown message: " + e)
	}
}

/**
 * Actor messages sent for step
 */
object StepManager {
	/**
	 * Message sent from the thread executing the process when the process completes.
	 *
	 * @param exitValue process completion status
	 */
	case class ProcessDone(exitValue: Int)

	/**
	 * Message sent to pause a flow.  This message is sent from an input for our step that needs a rest.  The request
	 * to pause the source is passed onto the StepManager managing the source for the flow.
	 *
	 * @param flow name of flow
	 */
	case class PauseSource(flow: String)

	/**
	 * Message sent to resume a flow.  This message is sent from an input for our step.  The request to resume
	 * the source is passed onto the StepManager managing the source for the flow.
	 *
	 * @param flow name of flow
	 */
	case class ResumeSource(flow: String)
}