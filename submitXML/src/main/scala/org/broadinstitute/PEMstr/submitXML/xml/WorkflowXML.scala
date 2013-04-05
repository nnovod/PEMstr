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

package org.broadinstitute.PEMstr.submitXML.xml

import XMLUtil._
import scala.xml.XML.loadFile
import xml.{PrettyPrinter, Node, Elem}
import org.broadinstitute.PEMstr.submitXML.xml.WorkflowXML._
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions._
import org.broadinstitute.PEMstr.common.workflow.RewindPipe.getRewindClass
import scala.Some

/**
 * @author Nathaniel Novod
 * Date: 11/5/12
 * Time: 3:07 PM
 *
 * Parse an XML workflow definition into a Workflow structure.  Basic verification of the XML is done along the way.
 * @constructor parse and create a Workflow data structure from an XML element
 * @param workflowXML Head XML element defining workflow
 */

class WorkflowXML (workflowXML: Elem) {
	/**
	 * @constructor parse and create a Workflow data structure from an XML file
	 * @param file file specification containing XML defining workflow
	 */
	def this(file: String) = this(loadFile(file))

	/**
	 * Get mandatory tag value associated with a xml node.  An exception is thrown if the tag does not exist.
	 *
	 * @param xml node with name tag
	 *
	 * @return string with name tag value
	 */
	private def getMandatoryTag(xml: Node, tag: String) = {
		val tagValue = getXMLTag(xml, tag)
		chkXML(tagValue.isDefined, tag + " attribute must be specified for " + xml.label)
		tagValue.get
	}

	/**
	 * Get name tag value associated with a xml node
	 *
	 * @param xml node with name tag
	 *
	 * @return string with name tag value
	 */
	private def getNameTag(xml: Node) = getMandatoryTag(xml, "@name")

	/**
	 * Get a list of flow to tag values (steps flow is going to) for a stream
	 *
	 * @param streamName name for stream flows are going to
	 * @param xml node for stream
	 * @param defaultBufferSettings buffer settings to use if none specified in to flow
	 *
	 * @return Seq of to stream names
 	 */
	private def getToFlows(streamName: String, xml: Node, defaultBufferSettings: BufferSettings) =
		(xml \ "flow").map(
			(flow) => getToFlow(streamName, flow, defaultBufferSettings))

	/**
	 * Get a list of flow to tag values (steps flow is going to) for a stream
	 *
	 * @param streamName name for stream flows are going to
	 * @param flow node for to flow
	 * @param defaultBufferSettings buffer settings to use if none specified in to flow
	 *
	 * @return Seq of to stream names
 	 */
	private def getToFlow(streamName: String, flow: Node, defaultBufferSettings: BufferSettings) =
		new InputStreamFlow(getMandatoryTag(flow, "@to"),
			InputStreamFile(streamName, getRewind(flow), getBufferSettings(flow, defaultBufferSettings)))

	/**
	 * Get buffering settings: Buffer size, Maximum size of free buffer cache, Buffer fill minimum percentage,
	 * Number of buffers to be pending before
	 *
 	 * @param stream node potentially containing buffering settings
	 * @param defaultBufferSettings buffer settings to set if none found in XML
	 *
	 * @return buffer settings
	 */
	private def getBufferSettings(stream: Node,
	                              defaultBufferSettings: BufferSettings= BufferSettings(None, None, None, None)) = {
		def bufferInt(options: Node, tag: String, min: Int) =
			getXMLTag(options, tag) match {
				case Some(i) => {
					val in = i.toInt
					chkXML(in >= min, "buffering " + tag + " must be minimum of " + min.toString)
					Some(in)
				}
				case None => None
			}

		getSingleNode(stream, "buffering") match {
			case Some(bufferOptions) => {
				BufferSettings(
					bufferInt(bufferOptions, "@size", 512),
					bufferInt(bufferOptions, "@cache", 1),
					bufferInt(bufferOptions, "@fillPct", 0),
					bufferInt(bufferOptions, "@pause", 2))
			}
			case None => defaultBufferSettings
		}
	}
	/**
	 * Get subnode that should only be there once or not at all.  An exception is thrown if the tag is there
	 * multiple times.
	 *
	 * @param xml node with subnode
	 * @param name label of subnode we're looking for
	 *
	 * @return optional node found
	 */
	private def getSingleNode(xml: Node, name: String) = {
		val nodesFound = xml \ name
		if (nodesFound.isEmpty) None else {
			chkXML(nodesFound.size == 1, name + " can only be specified once for " + xml.label)
			Some(nodesFound.head)
		}
	}

	/**
	 * Get subnode that should only be there once or not at all.  An exception is thrown if the tag is there
	 * multiple times.
	 *
	 * @param xml node with subnode
	 * @param name label of subnode we're looking for
	 *
	 * @return optional node found
	 */
	private def getMandatorySingleNode(xml: Node, name: String) = {
		val node = getSingleNode(xml, name)
		chkXML(node.isDefined, name + " must be specified for " + xml.label)
		node.get
	}

	/**
	 * Gets optional rewind specification for input stream.
	 *
 	 * @param flow flow XML node
	 * @return Optional rewind specification for flow to
	 */
	private def getRewind(flow: Node) = {
		getSingleNode(flow, "rewind") match {
			case Some(rewind) => {
				Some(getRewindClass(getMandatoryTag(rewind, "@datatype"),
					getMandatoryTag(rewind, "@records").toInt, getXMLint(rewind \ "@delay"),
					getXMLboolean(rewind \ "@reopen")))

			}
			case None => None
		}
	}

	/**
	 * Get flow to information for a join
	 *
	 * @param streamName name for stream join is for
	 * @param xml node for stream
	 * @param defaultBufferSettings buffer settings to use if none specified in to flow
	 *
	 * @return data about join input
 	 */
	private def getJoinToFlow(streamName: String, xml: Node, defaultBufferSettings: BufferSettings) = {
		val to = getMandatorySingleNode(xml, "flow")
		val joinAs = getSingleNode(xml, "joinAs")
		val joinSpec = if (joinAs.isDefined) JoinSpec(getXMLTag(joinAs.get, "@separator")) else JoinSpec(None)
		JoinInputStreamFlow(getMandatoryTag(to, "@to"),
			InputStreamFile(streamName, getRewind(to), getBufferSettings(to, defaultBufferSettings)), joinSpec)
	}

	/**
	 * Get output stream file data
	 *
 	 * @param stream xml for stream
	 *
	 * @return (streamName, Option[hiddenAsString], Option[checkPointAsString], isNotDirectPipeBoolean, bufferSettings)
	 */
	private def getOutputStreamFileData(stream: Node) = {
		(getNameTag(stream), getXMLstring(stream \ "hiddenAs"), getXMLstring(stream \ "checkpointAs"),
			getXMLboolean(stream \ "@noDirectPipes").getOrElse(false), getBufferSettings(stream))
	}

	/**
	 * Get split specification.  It must be part of a stream and refers to the source for the stream.
	 *
 	 * @param flow XML node
	 * @return optional split specification
	 */
	private def getSplitSpec(flow: Node) = {
		getSingleNode(flow, "splitAs") match {
			case Some(split) => {
				val sep = getXMLTag(split, "@separator")
				val paths = getXMLint(split \ "@instances")
				SplitSpec(paths, sep)
			}
			case None => SplitSpec(None, None)
		}
	}

	/* Check that all the tags in the XML are legit */
	chkXMLtags(workflowXML)

	/* Get workflow Name */
	private val workflowName = getNameTag(workflowXML)

	/* Get optional temp directory to use for workflow pipes etc. */
	private val tempDir = getXMLstring(workflowXML \ "tempDir")

	/* Get optional log directory to use for workflow process output etc. */
	private val logDir = getXMLstring(workflowXML \ "logDir")

	/**
	 * Get a step's command specification.
	 *
	 * @param command XML node for command
	 * @return contains command specification
	 */
	private def getCommand(command: Node) = {
		val args = command \ "args"
		val envVars = command \ "environmentVariables"
		StepCommand(args = (for (arg <- args \ "arg") yield trimXMLstring (arg, " ")),
			workingDirectory = getXMLstring(command \ "workingDir"),
			envVars = for (env <- envVars \ "variable") yield {
				val value = getMandatorySingleNode(env, "value")
				(getMandatoryTag(env, "@name") -> trimXMLstring(value))
			},
			continueOnError = getXMLboolean(command \ "@continueOnError"))
	}

	/* Map steps in XML to Step structure */
	private val steps = (workflowXML \ "steps" \ "step").map(step => {
		val resources = step \ "resources"
		Step(name = getNameTag(step),
			continueOnError = getXMLboolean(step \ "@continueOnError"),
			commands = (step \ "commands" \ "command").map(command => getCommand(command)),
			resources = StepResources(coresWanted = getXMLint(resources \ "@cpus"),
				suggestedScheduler = getXMLTag(resources, "@schedulerID"),
				gbWanted = getXMLint(resources \ "@gigs"),
				consumption = getXMLint(resources \ "@consumptionRate")))
	})

	/* Map streams in XML to StreamData structure */
	private val streams = (workflowXML \ "streams" \ "stream").map(stream => {
		val (name, hiddenAs, checkpointAs, isNotDirectPipe, bufferSettings) = getOutputStreamFileData(stream)
		new StreamData(
			new OutputStreamFile(
				outputStreamName = name,
				hiddenAs = hiddenAs,
				isNotDirectPipe = isNotDirectPipe,
				checkpointAs = checkpointAs,
				bufferOptions = bufferSettings),
			stepFrom = getMandatoryTag(stream, "@from"),
			toStreams = getToFlows(name, stream, bufferSettings))
	})

	/* Map joins in XML to JoinData structure */
	private val joins = (workflowXML \ "streams" \ "join").map(stream => {
		val (name, hiddenAs, checkpointAs, isNotDirectPipe, bufferSettings) = getOutputStreamFileData(stream)
		JoinData(
			joinFromStream = new OutputStreamFile(
				outputStreamName = name,
				hiddenAs = hiddenAs,
				isNotDirectPipe = isNotDirectPipe,
				checkpointAs = checkpointAs,
				bufferOptions = bufferSettings),
			fromStep = getMandatoryTag(stream, "@from"),
			toStream = getJoinToFlow(name, stream, bufferSettings))
	})

	/* Map splits in XML to SplitData structure */
	private val splits = (workflowXML \ "streams" \ "split").map(stream => {
		val toFlow = getMandatorySingleNode(stream, "flow")
		val (name, hiddenAs, checkpointAs, isNotDirectPipe, bufferSettings) = getOutputStreamFileData(stream)
		SplitData(
			splitFromStream = SplitOutputStreamFile(
				splitStreamName = name,
				splitCheckpointAs = checkpointAs,
				splitHiddenAs = hiddenAs,
				splitIsNotDirectPipe = isNotDirectPipe,
				split = getSplitSpec(stream),
				splitBufferOptions = bufferSettings),
			fromStep = getMandatoryTag(stream, "@from"),
			toStream =  getToFlow(getNameTag(stream), toFlow, bufferSettings))
	})

	/* Map tokens in XML to tokens for workflow */
	private val tokens = (workflowXML \ "variables" \ "variable").map(token => {
		val value = getMandatorySingleNode(token, "value")
		(getNameTag(token), trimXMLstring(value))
	}).toMap

	/* Put it all together into a Workflow structure */
	val workflow = Workflow(name = workflowName,
		tempDir = tempDir,
		logDir = logDir,
		steps = steps,
		dataFlows = streams ++ splits ++ joins,
		tokens = tokens)


	/**
	 * toString method to output XML as XML string
	 *
	 * @return XML string
	 */
	override def toString = {
		/**
		 * Create XML elements for flows in streams.
		 *
	 	 * @param strm stream data (can be join or split data in which case there will always be exactly one flow)
		 *
		 * @return XML elements for flows stream is flowing to
		 */
		def getFlowToXML(strm: StreamData) =
			for (flowTo <- strm.flowsTo) yield
				<flow to={flowTo.stepTo}>
					{if (flowTo.stream.rewind.isDefined)
						<rewind
						reopen={flowTo.stream.rewind.get.reopenFile.toString}
						datatype={flowTo.stream.rewind.get.dataType}
						records={flowTo.stream.rewind.get.records.toString}
						delay={flowTo.stream.rewind.get.delay.toString}/>}
					{getBuffering(flowTo.stream.bufferOptions)}
				</flow>

		/**
		 * Create XML elements for streams.
		 *
	 	 * @param strm stream data
		 *
		 * @return XML elements streams checkpointAs, hiddenAs and buffering
		 */
		def getStreamXML(strm: StreamData) = {
			{if (strm.fromStream.checkpointAs.isDefined)
				<checkPointAs>{strm.fromStream.checkpointAs.get}</checkPointAs>}
			{if (strm.fromStream.hiddenAs.isDefined)
				<hiddenAs>{strm.fromStream.hiddenAs.get}</hiddenAs>}
			{getBuffering(strm.fromStream.bufferOptions)}
		}

		/**
		 * Output XML for buffer settings
		 *
		 * @param bufferOptions contains buffer settings
		 *
		 * @return XML for buffer settings if one or more of the settings are set
		 */
		def getBuffering(bufferOptions: BufferSettings) = {
			{if (bufferOptions.isSpecified)
				<buffering
				size={bufferOptions.getBufferSize.toString}
				cache={bufferOptions.getCacheSize.toString}
				fillPct={bufferOptions.getFillPct.toString}
				pause={bufferOptions.getPauseSize.toString}
				/>
			}
		}

		/**
		 * Create XML for resources requested for a step
		 *
		 * @param resources resources requested for the step
		 *
		 * @return XML for step resource request
		 */
		def getResourcesToXML(resources: StepResources) =
				<resources schedulerID={if (resources.suggestedScheduler.isDefined)
				resources.suggestedScheduler.get.toString else ""}
				           gigs={if (resources.gbWanted.isDefined)
					           resources.gbWanted.get.toString else ""}
				           cpus={if (resources.coresWanted.isDefined)
					           resources.coresWanted.get.toString else ""}
				           consumptionRate={if (resources.consumption.isDefined)
					           resources.consumption.get.toString else ""}/>

		/**
		 * Create XML for split spec for a flow
		 *
		 * @param split specification for split
		 *
		 * @return XML for split stream's specification
	 	 */
		def getSplitSpecToXML(split: SplitSpec) = {
			val paths = split.instances match {
				case Some(p) => p.toString
				case None => ""
			}
				<splitAs
				separator={split.separator.getOrElse("")}
				instances={paths}/>
		}

		/**
		 * Create XML for a step's command
		 *
		 * @param command specification for command
		 *
		 * @return XML for step's command
	 	 */
		def getCommandToXML(command: StepCommand) = {
			<command continueOnError={command.isContinueOnError.toString}>
				{if (command.workingDirectory.isDefined)
				<workingDir>{command.workingDirectory.get}</workingDir>}
				{if (!command.envVars.isEmpty)
				<environmentVariables>
					{for (envVar <- command.envVars) yield {
						<variable name={envVar._1}>
							<value>{envVar._2}</value>
						</variable>}}
				</environmentVariables>}
				<args>
					{for (arg <- command.args) yield <arg>{arg}</arg> }
				</args>
			</command>
		}

		/* Output the XML string */
		new PrettyPrinter(132, 2).format(
		(<workflow name={workflow.name}>
			{if (tempDir.isDefined) <tempDir>{tempDir.get}</tempDir>}
			{if (logDir.isDefined) <logDir>{logDir.get}</logDir>}
			{if (!tokens.isEmpty) <variables>
				{for (token <- tokens) yield {
					<variable name={token._1}>
						<value>{token._2}</value>
					</variable>}}
			</variables>}
			<steps>
				{for (step <- steps) yield
				<step name={step.name} continueOnError={step.isContinueOnError.toString}>
					{if (step.resources.suggestedScheduler.isDefined || step.resources.gbWanted.isDefined ||
				  step.resources.coresWanted.isDefined || step.resources.consumption.isDefined)
					getResourcesToXML(step.resources)}
					<commands>
						{for (command <- step.commands) yield
						getCommandToXML(command)}
					</commands>
				</step>}
			</steps>
			<streams>
				{for (strm <- streams) yield
				<stream name={strm.name} from={strm.from} noDirectPipes={strm.fromStream.isNotDirectPipe.toString}>
					{getStreamXML(strm)}
					{getFlowToXML(strm)}
				</stream>}
				{for (split <- splits) yield
				<split name={split.name} from={split.from} noDirectPipes={split.fromStream.isNotDirectPipe.toString}>
					{val splitSpec = split.splitFromStream.split
				if (splitSpec.separator.isDefined || splitSpec.instances.isDefined)
				{getSplitSpecToXML(splitSpec)}}
					{getStreamXML(split)}
					{getFlowToXML(split)}
				</split>}
				{for (join <- joins) yield
				<join name={join.name} from={join.from} noDirectPipes={join.fromStream.isNotDirectPipe.toString}>
					{if (join.toStream.joinSpec.separator.isDefined)
						<joinAs separator={join.toStream.joinSpec.separator.get}/>}
					{getStreamXML(join)}
					{getFlowToXML(join)}
				</join>}
			</streams>
		</workflow>))
	}
}



object WorkflowXML {

	/**
	 * Token data
	 */

	/**
	 * Step in workflow
	 *
	 * @param name name given to step (it must be unique within the workflow)
	 * @param continueOnError if option set and true then workflow not aborted if this step completes with an error
	 * @param commands commands to execute for step
	 * @param resources Resources to be used for oommand
	 */
	case class Step(name: String,  private val continueOnError: Option[Boolean],
	                commands: Seq[StepCommand], resources: StepResources) {
		def isContinueOnError = if (continueOnError.isEmpty) false else continueOnError.get
	}

	/**
	 * Data flow between steps
	 *
	 * @param name name given to data (it must be unique within the workflow)
	 * @param from step name from which this data is being output
	 * @param flowsTo list of steps this data is being input to
	 */
	sealed abstract class Data(val name: String, val from: String, val flowsTo: Seq[InputStreamFlow])
	{
		val dataType: String
		def dataName: String = dataType +" \"" + name + "\""
		def dataArgRef = argRefToData(dataType, name)
	}

	/**
	 * Holds results of "to" for a stream
	 *
 	 * @param stepTo Name stream is going to
	 * @param stream characteristics of stream
	 */
	class InputStreamFlow(val stepTo: String, val stream: InputStreamFile) extends Serializable


	/**
	 * Holds results of "to" for a stream
	 *
 	 * @param joinTo Name stream is going to
	 * @param joinStream characteristics of stream
	 * @param joinSpec join specification
	 */
	case class JoinInputStreamFlow(private val joinTo: String,
	                               private val joinStream: InputStreamFile,
	                               joinSpec: JoinSpec) extends InputStreamFlow(joinTo, joinStream)

	/**
	 * Join data
	 *
	 * @param joinFromStream info about source of this stream
	 * @param fromStep step from which this data is being output
	 * @param toStream step this data is being input to
 	 */
	case class JoinData(private val joinFromStream: OutputStreamFile, private val fromStep: String,
	                    toStream: JoinInputStreamFlow)
	  extends StreamData(joinFromStream, fromStep, Seq(toStream)) {
		override val isJoin = true
	}

	/**
	 * Split data
	 *
	 * @param splitFromStream info about source of this stream
	 * @param fromStep step from which this data is being output
	 * @param toStream step this data is being input to
 	 */
	case class SplitData(splitFromStream: SplitOutputStreamFile, private val fromStep: String,
	                     private val toStream: InputStreamFlow)
	  extends StreamData(splitFromStream, fromStep, Seq(toStream)) {
		override val isSplit = true
	}
	/**
	 * Streaming data
	 *
	 * @param fromStream info about source of this stream
	 * @param stepFrom step name from which this data is being output
	 * @param toStreams list of steps this data is being input to
	 */
	class StreamData(val fromStream: OutputStreamFile, private val stepFrom: String,
	                 private val toStreams: Seq[InputStreamFlow])
		extends Data(fromStream.name, stepFrom, toStreams) with Serializable {
		override val dataType = "stream"
		val isSplit = false
		val isJoin = false
	}

	/**
	 * Workflow definition
	 *
	 * @param name name given to workflow
	 * @param tempDir specification of temporary directory to be used for pipes etc. for workflow
	 * @param logDir specification of directory used for process log files, etc.
	 * @param steps list of steps to be executed
	 * @param dataFlows list of flows between steps
	 * @param tokens name (keys) value (values) pairs for tokens
	 */
	case class Workflow(name: String, tempDir: Option[String],  logDir: Option[String],
	                    steps: Seq[Step], dataFlows: Seq[StreamData], tokens: Map[String, String]) {
		/**
		 * Get all the dataflows (streams) grouped by step names they come "from"
		 */
		lazy val flowsFrom = dataFlows.groupBy(_.from)

		/**
		 * Get all the dataflows (streams) grouped by step names they go "to"
	 	 */
		lazy val flowsTo = dataFlows.foldLeft(Map.empty[String, Seq[StreamData]])((toMap, data) => {
			data.flowsTo.foldLeft(toMap)((toMap, to) => {
				val toSoFarForStep = toMap.getOrElse(to.stepTo, Seq.empty[StreamData])
				toMap + (to.stepTo -> (data +: toSoFarForStep))
			})
		})
	}

	/**
	 * Get properly formatted string used as reference in command argument to a data flow
	 *
	 * @param dataType type of data (stream)
	 * @param dataName name of data flow
	 *
	 * @return {dataType=dataName}
	 */
	def argRefToData(dataType: String, dataName: String) = "{" + dataType + "=" + dataName + "}"

	/**
	 * Exception to signal if workflow is invalid
	 *
	 * @param err Error message
	 */
	case class BadWorkflowXML(err: String) extends Exception(err)

	/**
	 * Throw an exception if XML condition not true
	 *
	 * @param condition must be true for XML to be valid
	 * @param err error string to associate with exception thrown if condition is false
	 */
	def chkXML(condition: Boolean, err: => String) {if (!condition) throw BadWorkflowXML(err)}

	/**
	 * Structure to define valid XML tags
	 *
	 * @param label XML element label
	 * @param nxtLevel List of permissible elements at the next level
 	 */
	private case class WorkflowTags(label: String, nxtLevel: List[WorkflowTags])

	/**
	 * List of tags legal for all flow types
 	 */
	private val flowTags = List(WorkflowTags("hiddenAs", List.empty),
		WorkflowTags("checkpointAs", List.empty),
		WorkflowTags("buffering", List.empty),
		WorkflowTags("flow",
			List(WorkflowTags("rewind", List.empty),
				WorkflowTags("buffering", List.empty))))

	/* Init list of valid elements */
	//@TODO - someday use DTD
	private val workflowTags = WorkflowTags("workflow",
		List(WorkflowTags("tempDir", List.empty),
			WorkflowTags("logDir", List.empty),
			WorkflowTags("steps",
				List(WorkflowTags("step",
					List(WorkflowTags("resources", List.empty),
						WorkflowTags("commands",
							List(WorkflowTags("command",
								List(WorkflowTags("workingDir", List.empty),
									WorkflowTags("args",
										List(WorkflowTags("arg", List.empty))),
									WorkflowTags("environmentVariables",
										List(WorkflowTags("variable",
											List(WorkflowTags("value", List.empty))))))))))))),
			WorkflowTags("streams",
				List(WorkflowTags("split",
					WorkflowTags("splitAs", List.empty) +: flowTags),
					WorkflowTags("join",
						WorkflowTags("joinAs", List.empty) +: flowTags),
					WorkflowTags("stream", flowTags))),
			WorkflowTags("variables",
				List(WorkflowTags("variable",
					List(WorkflowTags("value", List.empty)))))))

	/**
	 * Check that all of XML has legitimate tags.  An exception will be thrown if any invalid tags are found
	 *
	 * @param xml Top element of xml
	 */
	def chkXMLtags(xml: Elem) {
		/**
		 * Check that XML tags are all legit
		 *
		 * @param xml top level XML element to check
		 * @param tags tags allowed within top level XML
	 	 */
		def checkXMLtags(xml: Elem, tags: WorkflowTags) {
			xml.child.foreach((node) => {
				node match {
					case e: Elem => {
						val nxtElem = tags.nxtLevel.find((t) => t.label == e.label)
						chkXML(nxtElem.isDefined,
							"Invalid element label found within \"" + xml.label + "\": " + "\"" + e.label + "\".  " +
								"Must be one of: " + tags.nxtLevel.map(_.label).mkString(", "))
						checkXMLtags(e, nxtElem.get)
					}
					case _ =>
				}
			})
		}
		/* Make sure that top tag is "workflow" */
		chkXML(xml.label == "workflow", "invalid XML in file - workflow not top label")
		/* Check rest of tags down the tree */
		checkXMLtags(xml, workflowTags)
	}
}
