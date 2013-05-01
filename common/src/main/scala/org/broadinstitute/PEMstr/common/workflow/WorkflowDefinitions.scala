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

import java.nio.ByteBuffer

/**
 * @author Nathaniel Novod
 * Date: 12/26/12
 * Time: 6:29 PM
 *
 * Structures (case class/object definitions) used to define a workflow
 */
object WorkflowDefinitions {
	/**
	 * Identifier for items that can be split such as streams and steps.
	 *
	 * @param baseName name assigned to stream/step (must be unique with the workflow for all streams or steps)
	 * @param instance optional instance # (for splits and joins)
 	 */
	class ItemInstance(val baseName: String, val instance: Option[Int]) extends Serializable {
		val name = getInstanceName(baseName, instance)
		override def hashCode() = name.hashCode()
	}

	/**
	 * Get the name used for an instance
	 *
 	 * @param baseName name of item
	 * @param instance optional instance of item
	 *
	 * @return name used for instance
	 */
	def getInstanceName(baseName: String, instance: Option[Int]) =
		if (instance.isDefined) baseName + "__" + instance.get.toString else baseName

	/**
	 * Pointer to the data in a step.
	 *
	 * @param stepName name of step that contains data
	 * @param dataName name of flow for data
	 */
	case class DataFilePointer(stepName: String, dataName: String)

	/**
	 * Input data file (file being written as input to a step)
	 *
	 * @param inputStreamName name assigned to input stream
	 * @param rewind optional rewind specification (resend of first x records of a specified format)
	 * @param bufferOptions settings for buffering to do for stream
	 * @param inputStreamInstance optional instance # (for joins)
	 */
	case class InputStreamFile(private val inputStreamName: String, rewind: Option[RewindPipe],
	                           bufferOptions: BufferSettings, private val inputStreamInstance: Option[Int] = None)
	  extends ItemInstance(inputStreamName, inputStreamInstance) with Serializable

	/**
	 * Input data flow (flow of output read from previous step to input written to current step)
	 *
	 * @param stream input flow coming into step
	 * @param sourceData output from previous step being streamed to this input
	 */
	class InputStreamDataFlow(val stream: InputStreamFile,
	                          val sourceData: DataFilePointer) extends Serializable

	/**
	 * Specification for a join
	 *
	 * @param separator separator between join files when creating a list of file names
	 */
	case class JoinSpec(separator: Option[String])

	/**
	 * Input data flow (flow of output read from previous step to input written to current step)
	 *
	 * @param joinStream input stream doing join
	 * @param joinSourceData pointer to output from previous steps being streamed into this join
	 * @param joinSpec specification for join
	 */
	case class JoinInputStreamDataFlow(private val joinStream: InputStreamFile,
	                                   private val joinSourceData: DataFilePointer, joinSpec: JoinSpec)
	  extends InputStreamDataFlow(joinStream, joinSourceData)

	/**
	 * Default # of streams to do split into
 	 */
	val defaultSplitPaths = 3

	/**
	 * Specification for a split.  If a separator is not specified then each name in the list is set as a separate
	 * argument for the command executed.
	 *
 	 * @param instances # of paths to do split into
	 * @param separator separator between split files when creating a list of file names
	 */
	case class SplitSpec(instances: Option[Int], separator: Option[String])

	/**
	 * Output data file (file being read as output from a step)
	 *
	 * @param splitStreamName name assigned to stream
	 * @param splitHiddenAs optional file specification used by program but not in command arguments
	 * @param splitIsNotDirectPipe true if direct pipes between steps can not be used
	 * @param splitCheckpointAs optional file specification to write out file for checkpointing
	 * @param split specification to split output into multiple flows
	 * @param splitBufferOptions settings for buffering to do for stream
	 * @param splitIgnoreEOF optional settings if initial EOF is to be ignored
	 * @param splitStreamInstance optional instance #
	 */
	case class SplitOutputStreamFile(private val splitStreamName: String, private val splitHiddenAs: Option[String],
	                                 private val splitIsNotDirectPipe: Boolean,
	                                 private val splitCheckpointAs: Option[String], split: SplitSpec,
	                                 private val splitBufferOptions: BufferSettings,
	                                 private val splitIgnoreEOF: Option[IgnoreEOF],
	                                 private val splitStreamInstance: Option[Int] = None)
	  extends OutputStreamFile(splitStreamName, splitHiddenAs, splitIsNotDirectPipe,
		  splitCheckpointAs, splitBufferOptions, splitIgnoreEOF, splitStreamInstance)

	/**
	 * Output data file (file being read as output from a step)
	 *
	 * @param outputStreamName name assigned to stream
	 * @param hiddenAs optional file specification used by program but not in command arguments
	 * @param isNotDirectPipe true if direct pipes between steps can not be used
	 *                        (sometimes needed to avoid process blocking when opening multiple streams between steps)
	 * @param checkpointAs optional file specification to write out file for checkpointing
	 * @param bufferOptions settings for buffering to do for stream
	 * @param ignoreEOF optional settings if initial EOF is to be ignored
	 * @param outputStreamInstance optional instance # (for splits)
	 */
	class OutputStreamFile(private val outputStreamName: String, val hiddenAs: Option[String],
	                       val isNotDirectPipe: Boolean, val checkpointAs: Option[String],
	                       val bufferOptions: BufferSettings, val ignoreEOF: Option[IgnoreEOF],
	                       private val outputStreamInstance: Option[Int] = None)
	  extends ItemInstance(outputStreamName, outputStreamInstance) with Serializable

	/**
	 * Resources requested for a step
	 *
	 * @param coresWanted # of cores wanted to run the step
	 * @param gbWanted # of gigabytes of memory wanted to run the step
	 * @param suggestedScheduler optional ID of scheduler suggested to run step
	 * @param consumption optional consumption # (0-5)
	 * 0 means it can not start processing till all input arrives 5 means it can process input as soon as it arrives
	 */
	case class StepResources(coresWanted: Option[Int], gbWanted: Option[Int],
	                         suggestedScheduler: Option[String], consumption: Option[Int])

	/**
	 * Resources requested for a step
	 *
	 * @param args sequence of command and argument strings
	 * @param workingDirectory optional working directory
	 * @param envVars environment variables (name -> value)
	 * @param continueOnError true if we continue to the next command within step even if the command fails
	 */
	case class StepCommand(args: Seq[String], workingDirectory: Option[String],
	                       envVars: Seq[(String, String)], private val continueOnError: Option[Boolean]) {
		/**
		 * Enforce default of false if None
		 * @return true if specified we should continue to next step command if current command fails
		 */
		def isContinueOnError = !(continueOnError.isEmpty || !continueOnError.get)
	}

	/**
	 * Step definition
	 *
	 * @param stepName name assigned to step (must be unique within the workflow)
	 * @param isContinueOnError if true then workflow should not be aborted if this step reports an error
	 * @param commands sequence of commands to execute
	 * @param inputs input data to step keyed by flow name
	 * @param outputs output data from step keyed by flow name
	 * @param resources resources requested for step
	 * @param stepInstance instance of step (if step has been split into multiple instances)
	 */
	case class StepDefinition(private val stepName: String, isContinueOnError: Boolean,
	                          commands: Seq[StepCommand],
	                          inputs: Map[String, InputStreamDataFlow],
	                          outputs: Map[String, OutputStreamFile],
	                          resources: StepResources,
	                          private val stepInstance: Option[Int] = None)
		extends ItemInstance(stepName, stepInstance)

	/**
	 * Workflow definition
	 *
	 * @param name name assigned to workflow
	 * @param tmpDir specification for temporary directory
	 * @param logDir specification for directory for logging
	 * @param steps map of step names (must be unique) to step definitions
	 * @param tokens map of token names (must be unique) to token values
	 */
	case class WorkflowDefinition(name: String, tmpDir: String, logDir: String,
	                              steps: Map[String, StepDefinition], tokens: Map[String, String]) {
		/**
		 * Make a graph that can be looked at as an easy way to traverse flows (edges) between steps (nodes)
		 *
		 * @return a directed graph with steps as nodes and streams' flows as edges
		 */
		def toGraph = {
			import scalax.collection.Graph
			import scalax.collection.edge.Implicits._

			/* Get the steps as nodes */
			val nodes = steps.keys
			/* Get flows as edges */
			val edges =
				steps.flatMap((step) => {
					val (stepName, stepEntry) = step
					stepEntry.inputs.map((flow) => {
						val (flowName, flowEntry) = flow
						(flowEntry.sourceData.stepName ~+#> stepName)(flowName)
					})
				})
			/* Create the graph */
			Graph.from(nodes, edges)
		}
	}

	/**
	 * Default buffer size for buffering data.
	 */
	private val bufferDefaultSize = 65536

	/**
	 * Default max number of buffers to keep free
	 */
	private val bufferDefaultCacheSize = 150

	/**
	 * Default minimum percentage of buffer to fill before sending the read data onward
	 */
	private val bufferDefaultFillPct = 80

	/**
	 * Default max # of buffers to hold pending (received but not sent onwards, typically received over a socket
	 * but not yet able to send it over a pipe because the pipe is busy) before trying to pause the stream source.
	 */
	private val bufferDefaultPause = 150

	/**
	 * Default level of multi buffering
	 */
	private val bufferDefaultMultiLvl = 2

	/**
	 * Buffer settings for transmitting data over streams.
	 *
 	 * @param size byte size of buffers
	 * @param cache maximum # of buffers to keep cached for reuse (above this # is freed back to system pool)
	 * @param fillPct Minimum % to fill buffer before transmitting buffer
	 * @param pause max # of buffers to hold pending before trying to pause the stream source
	 * @param multiLevel # of buffers to use for multi buffering
	 */
	case class BufferSettings(private val size: Option[Int], private val cache: Option[Int],
	                          private val fillPct: Option[Int], private val pause: Option[Int],
	                          private val multiLevel: Option[Int]) {
		/**
		 * Get byte size to use for buffers.
		 *
		 * @return buffer byte size
		 */
		def getBufferSize = size.getOrElse(bufferDefaultSize)

		/**
		 * Get max # of buffers to keep cached for reuse
		 *
		 * @return max # of buffers to keep in reuse cache
		 */
		def getCacheSize = cache.getOrElse(bufferDefaultCacheSize)

		/**
		 * Get minimum % to fill buffer before transmitting it
		 *
		 * @return minimum percentage to fill buffer before transmitting it
		 */
		def getFillPct = fillPct.getOrElse(bufferDefaultFillPct)

		/**
		 * Get # of buffers to use for multi buffering
		 *
		 * @return # of buffers to use for multi buffering
		 */
		def getMultiLvl = multiLevel.getOrElse(bufferDefaultMultiLvl)

		/**
		 * Is buffer filled to wanted level?
		 *
		 * @param data buffer with data
		 *
		 * @return true if buffer filled with data to wanted percentage
		 */
		def isFilled(data: ByteBuffer) = {
			/* Quick optimization that also protects against 0 capacity (remaining must be >= 0 and <= capacity) */
			data.remaining() == 0 || {
				val pctFilled = (data.capacity() - data.remaining()) * 100 / data.capacity()
				val pctDesired = math.min(100, getFillPct)
				pctFilled > pctDesired
			}
		}

		/**
		 * Are any buffering parameters specified?
		 *
 		 * @return true if one or more of the parameters specified
		 */
		def isSpecified = size.isDefined || cache.isDefined || fillPct.isDefined ||
			pause.isDefined || multiLevel.isDefined

		/**
		 * Get max # of buffers to have pending before asking source to pause.
		 *
		 * @return max # of buffers to have pending before asking source to pause
		 */
		def getPauseSize = pause.getOrElse(bufferDefaultPause)
	}

	/**
	 * Specification to ignore initial EOF in output stream
	 *
	 * @param delay optional # of seconds to delay reopen
	 * @param reopen true if reopen should be done when EOF is seen
	 */
	case class IgnoreEOF(private val delay: Option[Int], private val reopen: Option[Boolean]) {
		/**
		 * Get # of seconds to delay between a close and reopen (defaults to 3)
		 *
		 * @return # of seconds to delay between a close and reopen
		 */
		def getDelay = delay.getOrElse(3)

		/**
		 * Is a reopen wanted?
		 *
		 * @return true if a reopen of the stream is wanted when end-of-file is seen
		 */
		def isReopen = reopen.getOrElse(true)
	}

	/**
	 * Exception to signal if workflow is invalid
	 *
	 * @param err Error message
	 */
	case class InvalidWorkflow(err: String) extends Exception(err)
}
