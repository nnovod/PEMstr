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

/**
 * @author Nathaniel Novod
 * Date: 11/26/12
 * Time: 10:37 AM
 *
 * Convert the XML workflow definition to a WorkflowDefinition structure that can be sent to the workflow manager
 * to execute the workflow.  Along the way we check out the validity of the XML and associate inputs and outputs
 * of each step with the individual step definitions.  Note this trait must be mixed in with WorkflowXML.
 */
import org.broadinstitute.PEMstr.submitXML.xml.WorkflowXML.{JoinData,Data,StreamData,chkXML,argRefToData}
import org.broadinstitute.PEMstr.common.workflow.DirectoryDefaults.{logDirDefault,tmpDirDefault}
import org.broadinstitute.PEMstr.common.workflow.WorkflowSplitJoin.splitWorkflow
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.
{JoinInputStreamDataFlow,StepDefinition,InputStreamDataFlow,WorkflowDefinition,DataFilePointer}
import org.broadinstitute.PEMstr.common.StepCommandArgTokens.matchStreamsAndVariables

trait WorkflowXMLToDefinition {

	/**
	 * This trait must be mixed in with WorkflowXML
 	 */
	this: WorkflowXML =>

	/**
	 * Get the workflow definition as a WorkflowDefinition structure that can be sent to the WorkflowManager
	 * to execute the workflow.  Before creating the workflow definition we do a series of validity checks including:
	 *
	 * - Make sure step and dataflow names are unique
	 *
	 * - Check that all "from" and "to" step names in flows are valid step names
	 *
	 * - Check that replacement strings ({stream=<streamName>}) in command args are valid
	 *   (have a known stream/file name and that the stream/file referenced is from or to the step associated with
	 *   the command args)
	 *
	 * - Check that all streams associated with a step are referenced in the step's command args
	 *
	 * - Check that each stream has at least one destination step
	 *
	 * If any validity check fails an exception is thrown
	 *
	 * @return WorkFlowDefinition to be used to execute workflow
 	 */
	lazy val workflowDefinition = {
		/* Get all the steps grouped by step names  - should only be one step per name */
		val stepsMap = workflow.steps.groupBy(_.name)

		/* Get all the dataflows grouped by data names */
		val flowsMap = workflow.dataFlows.groupBy(_.name)

		/* Look through entries grouped by name to make sure there aren't multiple entries for any names */
		def checkUnique[A](map: Map[String,  Seq[A]], nameTypes: String) {
			map foreach {
				case (name, entries) =>
					chkXML(entries.length == 1,
						nameTypes + " names must be unique: \"" + name + "\" is used in multiple " + nameTypes + "s")
			}
		}

		/* Report that a step name associated with a list of flows is not a valid step name */
		def stepNotFound(entry: (String, Seq[StreamData]), usage: String) = {
			val (step, data) = entry
			"Unknown step name \"" + step + "\" specified in " + usage + " attribute of " +
				data.map(_.dataName).mkString(",")
		}

		/* Check that all replacement tokens in step's args are valid and that all flows exist as replacement tokens */
		def checkArgNamesOK(stepName: String,  args: Seq[String]) {
			def chkNameExists[DT <: Data](name: String, flows: Seq[DT], fileType: String) {
				chkXML(flows.exists((d) => d.name == name && d.isInstanceOf[DT]),
					"Can not find " + fileType + " name to substitute for " + argRefToData(fileType, name))
			}

			/* Check that stream name is going from or to a step */
			def chkNameAssociatedWithStep(streamName: String, stepName: String) {
				def chkExists(flows: Map[String, Seq[StreamData]]) = {
					val stepList = flows.get(stepName)
					stepList.exists((dataList) => dataList.exists(_.name == streamName))
				}
				chkXML(chkExists(workflow.flowsFrom) || chkExists(workflow.flowsTo),
					"Could not find stream/file \"" + streamName + "\" from/to step \"" + stepName + "\"")
			}

			/*
			 * Loop through command arguments to see that all stream references are valid (i.e., stream name
			 * exists and the stream is going to and/or from the step) and that the token references are valid.
			 */
			args.foreach((arg) => {
				matchStreamsAndVariables(arg, (streamName) => {
					chkNameExists[StreamData](streamName, workflow.dataFlows, "stream")
					chkNameAssociatedWithStep(streamName, stepName)
				}, (tokenName, sysVar) => {
					chkXML(sysVar || workflow.tokens.get(tokenName).isDefined, err = "Invalid token name \"" +
					  tokenName + "\" in args for step " + stepName)
				})
			})

			/* Make sure all data elements in a list are referenced in the step's command arguments */
			def findDataInArgs(dataList: Seq[StreamData], canBeHidden: Boolean) {
				dataList.foreach((entry) => {
					if (!canBeHidden || entry.fromStream.hiddenAs.isEmpty) {
						chkXML(args.exists((arg) => arg.contains(entry.dataArgRef)),
							"Could not find needed " + entry.dataArgRef +
							  " in step \"" + stepName + "\" command arguments")
					}
				})
			}

			/* Check that streams going from and to a step are referenced in the step's command arguments */
			findDataInArgs(workflow.flowsFrom.getOrElse(stepName, Seq.empty[StreamData]), canBeHidden = true)
			findDataInArgs(workflow.flowsTo.getOrElse(stepName, Seq.empty[StreamData]), canBeHidden = false)

			/* Check that each stream data is coming from has at least one "to" flow */
			workflow.dataFlows.foreach((d) => chkXML(d.flowsTo.size != 0,
				"No \"Flow To\" entries found for " + d.dataName))
		}

		/* Create source reference (step data coming from) for data going to a step */
		def getSource(toData: StreamData) = DataFilePointer(toData.from, toData.name)

		/**
		 * Get the input stream info for a given step/stream.
		 *
		 * @param stepName name of step stream is going to
		 * @param stream stream definition
		 * @return input stream information for stream
		 */
		def getToStream(stepName: String, stream: StreamData) = {
			val toStream = stream.flowsTo.find(_.stepTo == stepName)
			assert(toStream.isDefined, "Lost flowto stream for step " + stepName)
			toStream.get.stream
		}

		/**
		 * Get a directory - if specified use that (taking off any trailing "/"), otherwise use default.
		 *
 		 * @param specified optional specified directory
		 * @param default default to use if none specified
		 * @return directory specification
		 */
		def getDir(specified: Option[String], default: String) = {
			specified match {
				case Some(dir) => if (dir.endsWith("/")) dir.substring(0, dir.length-1) else dir
				case None => default
			}
		}
		/* Check uniqueness of step and flow names */
		checkUnique(stepsMap, "step")
		checkUnique(flowsMap, "flow")

		/* Check step names that streams flow from and then step names flowed to */
		workflow.flowsFrom.foreach((f) => chkXML(stepsMap.exists(_._1 == f._1), stepNotFound(f, "from")))
		workflow.flowsTo.foreach((f) => chkXML(stepsMap.exists(_._1 == f._1), stepNotFound(f, "flow to")))

		/* Check that any token names found in a string are found in the workflow definition */
		def checkTokenValues(str: Option[String]) {
			str match {
				case Some(strFound) =>
					matchStreamsAndVariables(strFound, (streamName) => {
						chkXML(condition = false,
							err = "stream (\"" + streamName + "\") type found where only tokens allowed")
					}, (tokenName, sysVar) => {
						chkXML(sysVar || workflow.tokens.get(tokenName).isDefined, err = "Invalid token name \"" +
						  tokenName + "\" found")
					})
				case None =>
			}
		}

		/* Check that any tokens used in hidden as and checkpoint as are legit */
		workflow.dataFlows.foreach((d) => {
			checkTokenValues(d.fromStream.hiddenAs)
			checkTokenValues(d.fromStream.checkpointAs)
		})

		/**
		 * Get the input data flow for a stream going to a specified step.
		 *
 		 * @param toData stream definition
		 * @param stepName name of step data is going to
		 *
		 * @return description of input flow going to step
		 */
		def getInputStreamDataFlow(toData: StreamData, stepName: String) = {
			/* Get input stream file */
			val toStream = getToStream(stepName, toData)
			/* Get pointer to source of data */
			val source = getSource(toData)
			toData match {
				case JoinData(_, _, joinInput) =>
					JoinInputStreamDataFlow(toStream, source, joinInput.joinSpec)
				case _ => new InputStreamDataFlow(toStream, source)
			}
		}

		/* Finally return the workflow definition */
		val workflowDef = WorkflowDefinition(name = workflow.name,
			/* If temporary directory is provided in the XML then use it, otherwise default to "/var/tmp" */
			tmpDir = getDir(workflow.tempDir, tmpDirDefault),
			logDir = getDir(workflow.logDir, logDirDefault),
			tokens = workflow.tokens,
			steps = stepsMap.map((stepEntry) => { // Map the steps to StepDefinitions
				val (stepName, stepDataList) = stepEntry
				/* Already checked that there's only one entry per list so we flatten it here */
				val stepData = stepDataList.head
				/* Go check that proper streaming tokens are somewhere in the command arguments */
				val allArgs = stepData.commands.foldLeft(Seq.empty[String])((tot, next) => tot ++ next.args)
				checkArgNamesOK(stepName, allArgs)
				/* Create the step definition */
				stepName -> StepDefinition(stepName = stepName,
					isContinueOnError = stepData.isContinueOnError,
					/* Map input/output files from Data to InputStreamDataFlow/Datafile */
					inputs = workflow.flowsTo.getOrElse(stepName, List.empty).map((toData) => {
						toData.name -> getInputStreamDataFlow(toData, stepName)}).toMap,
					outputs = workflow.flowsFrom.getOrElse(stepName, List.empty).map(
						(fromData) => fromData.name -> fromData.fromStream).toMap,
					commands = stepData.commands,
					resources = stepData.resources
				)})
		)
		/* Make sure workflow is acyclic */
		val cycles = workflowDef.toGraph.findCycle
		chkXML(cycles.isEmpty, "Workflow can not be cyclic.  Workflow "
			+ workflow.name + " contains " + cycles.get.toString)
		/* Get the split/joins just to check that there's no problem with them but return the original definition */
		splitWorkflow(workflowDef)
		/* Leave actual split/joins to central scheduler to allow more flexibility at execution time */
		workflowDef
	}
}
