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

import collection.immutable.Stack
import org.broadinstitute.PEMstr.common.util.Util.plural
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions._
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.SplitOutputStreamFile
import scala.Some
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.WorkflowDefinition
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.SplitSpec
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.StepDefinition
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.InvalidWorkflow
import org.broadinstitute.PEMstr.common.workflow.WorkflowDefinitions.JoinInputStreamDataFlow

/**
 * @author Nathaniel Novod
 * Date: 2/14/13
 * Time: 3:59 PM
 *
 * Analyze a workflow's split/join structure and create a new workflow with proper step and flow instances created
 * for splits and joins to occurs.
 *
 */
object WorkflowSplitJoin {

	/**
	 * Make new workflow making multiple step instances and flows where splits and joins occurs.  New instance names
	 * are simply the original name with a "&#95;&#95;" and instance number appended.  Following is a simple example of how
	 * a split is redone:
	 *
	 * If the original flow is from step S1 to step S2 being split two ways via flow FS (S1--via FS-->S2) that becomes
	 * the two paths:
	 * S1--via FS__1-->S2__1 and S1--via FS__2-->S2__2
	 *
	 * A simple join example is:
	 *
	 * If the original flow is from step S2 to step S3 being joined via flow FJ (S2--via FV-->S3) from a previous
	 * two-way split the paths become:
	 * S2__1--via FV__1-->S3 and S2__2--via FV__2-->S3
	 *
	 * Nested splits are allowed.  For example S1 can split 2-ways into S2 which can split 3-ways into S3 resulting
	 * in 2 instances of S2 and 6 (2*3) instances of S3.  If S3 then did a join into S4 there are 2 instances of S4.
	 * Another join results in a single step instance for the target of the join.
	 *
	 * Step names must remain unique across the entire workflow.  Flow names leading to or from a step must be unique
	 * within the step but can occur in paths connected to other steps.
	 *
 	 * @param workflow original workflow definition
	 *
	 * @return new workflow definition with step and flow instances created for splits and joins found
	 */
	def splitWorkflow(workflow: WorkflowDefinition) = {

		/* Get split information for path steps are on */
		val splitInfo = getStepSplits(workflow)

		/**
		 * Get the number of instances there should be for a step taking into account splits and joins.
		 *
 		 * @param step name of step
		 *
		 * @return number of instances to create for the step
		 */
		def numberStepInstances(step: String) =
			checkStepInstances(step, splitInfo(step))

		/**
		 * Make an new map of the original outputs for a step taking into account splits and joins done.
		 * If the output is being split then we need to make one flow for each split path.  If the output is
		 * leading into a join then we need to make a new output entry that properly identifies the stream
		 * instance based on which flow this output will use into the join.  Finally, if the output is being
		 * neither split nor joined then we leave the original output entry untouched.
		 *
		 * @param step step definition
		 * @param stepInstance instance of step
		 *
		 * @return updated map of outputs (outputInstanceName -> OutputStreamFile)
		 */
		def splitOutputs(step: StepDefinition, stepInstance: Option[Int]) = {
			/* Get step name and # of instances for step */
			val stepName = step.baseName
			val ourInstances = numberStepInstances(stepName)

			/**
			 * Given an output stream of one step, find the number of instances there are of target steps for the stream
			 * (steps where the stream is the input).
			 *
	 		 * @param output output stream
			 *
			 * @return # of instances for target steps of stream
			 */
			def numberTargetInstances(output: OutputStreamFile) = {
				/* Get the graph for the workflow and find all the edges for the output stream */
				val graph = workflow.toGraph
				val edges = graph.edges.filter(_.label == output.name)
				/* Map the set of edges found to a set of the # of instances for the directed edge's targets */
				val instances = edges.map((edge) => numberStepInstances(edge._2))
				/*
				 * All targets for the flow should have the same # of instances so the newly created set
				 * should have only 1 member
				 */
				assert(instances.size == 1,
					"Target of flow " + output.name + " has differing number of instances " + instances.toString)
				/* Return one value found for # of instances */
				instances.head
			}

			/**
			 * Make a map of the outputs that a single original output becomes once splits and joins are taken
			 * into account.
			 *
 			 * @param stream original output stream (before split/joins)
			 *
			 * @return map of outputs (outputInstanceName -> OutputStreamFile) coming from original output
			 */
			def updateOutputStream(stream: OutputStreamFile) = {
				stream match {
					/**
					 * If a split stream then we need one new output stream for each instance of the split
					 */
					case sos: SplitOutputStreamFile => {
						(for (i <- 0 until sos.split.instances.getOrElse(defaultSplitPaths)) yield {
							val newStream = SplitOutputStreamFile(sos.name, sos.hiddenAs,
								sos.isNotDirectPipe, sos.checkpointAs, sos.split, sos.bufferOptions, Some(i))
							newStream.name -> newStream
						}).toMap
					}

					/**
					 * If a join stream then we need to make sure the stream is set to the proper instance to match
					 * the input stream of the target step that this stream will match up with.
					 */
					case os: OutputStreamFile => {
						val targetInstances = numberTargetInstances(os)
						assert(!(targetInstances > ourInstances),
							"Target of non-split flow " + stream.name + " has more instances (" +
							  targetInstances.toString + ") than source (" + ourInstances.toString + ")")
						/* If stream has same number of sources and targets then there must not be a join */
						if (targetInstances == ourInstances) { // Neither split nor join - use original stream
							Map(stream.name -> stream)
						} else {
							/* Get # of streams being joined to target of this original stream */
							val joinPaths = ourInstances/targetInstances
							/* Create instance of streams being joined that we are using */
							val flowInstance = stepInstance.getOrElse(0) % joinPaths
							val newStream = new OutputStreamFile(os.name, os.hiddenAs, os.isNotDirectPipe,
								os.checkpointAs, os.bufferOptions, Some(flowInstance))
							Map(newStream.name -> newStream)
						}
					}
				}
			}

			/* Go through the outputs putting together a new map of all the streams calculated from split and joins */
			step.outputs.foldLeft(Map.empty[String, OutputStreamFile])((map, flowEntry) => {
				map ++ updateOutputStream(flowEntry._2)
			})

		}

		/**
		 * Make an new map of the original inputs for a step taking into account splits and joins done.
		 * If the input comes from a split then we need to make a new input entry that properly identifies
		 * the stream instance this input will be taking from the split.  If the input is going into a join
		 * then we need to make new input entries for each stream instance being joined.  Finally, if the input
		 * stream is being neither split nor joined then we just need to make sure that the source step instance
		 * for the flow is properly identified.
		 *
		 * @param step step definition
		 * @param stepInstance instance of step
		 *
		 * @return updated map of outputs (outputInstanceName -> OutputStreamFile)
		 */
		def splitInputs(step: StepDefinition, stepInstance: Option[Int]) = {
			val stepName = step.baseName

			/**
			 * Create a new input stream data flow based on a step's original input stream data flow with new instance
			 * information.
			 *
	 		 * @param flow step's original data flow
			 * @param flowInstance instance # for new input stream data flow being created
			 * @param sourceInstance optional instance # for source (output of previous step) for data flow
			 *
			 * @return new input stream data flow modified with instance information
			 */
			def makeNewDataFlow(flow: InputStreamDataFlow, flowInstance: Int,
			                    sourceInstance: Option[Int]) = {
				val stream = InputStreamFile(flow.stream.baseName, flow.stream.rewind,
					flow.stream.bufferOptions, Some(flowInstance))
				val source = DataFilePointer(getInstanceName(flow.sourceData.stepName, sourceInstance), stream.name)
				flow match {
					case jis: JoinInputStreamDataFlow =>
						JoinInputStreamDataFlow(stream, source, jis.joinSpec)
					case is: InputStreamDataFlow =>
						new InputStreamDataFlow(stream, source)
				}
			}

			/**
			 * Create a new input stream data flow for one of a data flow's instances being joined.
			 *
	 		 * @param flow step's original data flow
			 * @param flowInstance instance # for new input stream data flow being created
			 * @param sourceInstance instance # for source (output of previous step) for data flow
			 *
			 * @return new input stream data flow modified with instance information
			 */
			def newJoinDataFlow(flow: InputStreamDataFlow, flowInstance: Int, sourceInstance: Int) =
				makeNewDataFlow(flow, flowInstance, Some(sourceInstance))

			/**
			 * Create a new input stream data flow for one of a data flow's instances being split.
			 *
	 		 * @param flow step's original data flow
			 * @param flowInstance instance # for new input stream data flow being created
			 * @param sourceInstance optional instance # for source (output of previous step) for data flow
			 *
			 * @return new input stream data flow modified with instance information
			 */
			def newSplitDataFlow(flow: InputStreamDataFlow, flowInstance: Int, sourceInstance: Option[Int]) =
				makeNewDataFlow(flow, flowInstance, sourceInstance)

			/**
			 * Make a map of the inputs that a single original input becomes once splits and joins are taken
			 * into account.
			 *
 			 * @param dataFlow original input dataflow (before split/joins)
			 *
			 * @return map of inputs (inputInstanceName -> InputStreamDataFlow) coming from original output
			 */
			def updateInputDataFlow(dataFlow: InputStreamDataFlow) = {
				val sourceStep = dataFlow.sourceData.stepName
				val flowName = dataFlow.stream.name
				val sourceInstances = numberStepInstances(sourceStep)
				val ourInstances = numberStepInstances(stepName)

				/**
				 * If # of source and target instances are the same for the dataflow then neither splitting nor
				 * joining is taking place so we just make sure that the source and target step instances align.
				 *
				 * If the number of source instances is greater than the number of target instances then we must
				 * be doing a join.  A new dataflow is setup for each stream instance going into the join.
				 *
				 * If the number of target instances is greater than the number of source instances then we must
				 * be doing a split.  We need to make this input data flow be the proper instance to align properly
				 * with the split output instance being sent to this step instance.
				 */
				if (ourInstances == sourceInstances) { // No splitting or joining
					if (stepInstance.isDefined) {
						Map(flowName -> new InputStreamDataFlow(dataFlow.stream,
							DataFilePointer(getInstanceName(sourceStep, stepInstance), flowName)))
					} else {
						Map(flowName -> dataFlow)
					}
				} else if (ourInstances < sourceInstances) { // Joining multiple inputs
					val joinPaths = sourceInstances/ourInstances
					(for (flowInstance <- 0 until joinPaths) yield {
						val sourceInstance = (stepInstance.getOrElse(0) * joinPaths) + flowInstance
						val newDataFlow = newJoinDataFlow(dataFlow, flowInstance, sourceInstance)
						newDataFlow.stream.name -> newDataFlow
					}).toMap
				} else { // Output was split - we get one of splits
					val splitPaths = ourInstances/sourceInstances
					val flowInstance = stepInstance.getOrElse(0) % splitPaths
					val sourceInstance = if (sourceInstances > 1) Some(stepInstance.getOrElse(0) / splitPaths) else None
					val newDataFlow = newSplitDataFlow(dataFlow, flowInstance, sourceInstance)
					Map(newDataFlow.stream.name -> newDataFlow)
				}
			}

			/* Go through the inputs putting together a new map of all the streams calculated from split and joins */
			step.inputs.foldLeft(Map.empty[String, InputStreamDataFlow])((map, flowEntry) => {
				map ++ updateInputDataFlow(flowEntry._2)
			})
		}

		/* Create new instances of the steps where splits make the need for multiple step instances */
		val newSteps = workflow.steps.flatMap((step) => {
			/* Get split entry for step and then find out # of instances of step there should be */
			val (stepName, stepEntry) = step
			val stepInstances = numberStepInstances(stepName)
			/* Create map of instanceName -> StepDefinition for instance */
			if (stepInstances <= 1) { /* Keep single instance, but make sure flow's joins/splits taken into account */
				Map(stepName ->
				  StepDefinition(stepName = stepName, isContinueOnError = stepEntry.isContinueOnError,
					commands = stepEntry.commands, inputs = splitInputs(stepEntry, None),
					outputs = splitOutputs(stepEntry, None), resources = stepEntry.resources))
			} else { /* Need to create multiple instances of step */
				(for (i <- 0 until stepInstances) yield getInstanceName(stepEntry.baseName, Some(i)) ->
					StepDefinition(stepName = stepEntry.baseName, isContinueOnError = stepEntry.isContinueOnError,
						commands = stepEntry.commands, inputs = splitInputs(stepEntry, Some(i)),
						outputs = splitOutputs(stepEntry, Some(i)), resources = stepEntry.resources,
						stepInstance = Some(i))).toMap
			}
		})
		/* Return new workflow definition */
		WorkflowDefinition(name = workflow.name, tmpDir = workflow.tmpDir, logDir = workflow.logDir,
			steps = newSteps, tokens = workflow.tokens)
	}

	/**
	 * Specification for a split path
	 *
	 * @param flow flow that originated split
	 * @param spec specification for split
	 */
	case class SplitInfo(flow: String, spec: SplitSpec)

	/**
	 * Check that all the split paths for a step are compatible.  If two paths lead to a different level of
	 * splitting for the step then the paths are considered incompatible.
	 *
	 * @param step name of step
	 * @param entries different split paths that lead into step
	 *
	 * @return # of instances need for splits of step (0 if no splits)
	 */
	private def checkStepInstances(step: String, entries: List[Array[SplitInfo]]): Int = {
		def splitSize(splitInfo: Array[SplitInfo]) =
			splitInfo.foldLeft(1)((size, split) => size * split.spec.instances.getOrElse(defaultSplitPaths))

		/**
		 * Check that all the entries have the same # of splits for the step
		 */
		if (entries.size > 1) {
			val (splitSize0, splitSize1) = (splitSize(entries(0)), splitSize(entries(1)))
			if (splitSize0 != splitSize1) {
				throw InvalidWorkflow("Step " + step + " has incompatible splits leading into it: split into " +
				  plural(splitSize0, "path") + " and " + plural(splitSize1, "path"))
				splitSize0 // Return one of sizes - shouldn't get here
			} else {
				checkStepInstances(step, entries.tail)
			}
		} else if (entries.size == 1) splitSize(entries(0)) else 1
	}

	/**
	 * Get the split/join levels for steps in a workflow.  We make sure the workflow is not cyclic and that the steps
	 * do not have incompatible split levels leading into them (e.g., one path that splits the step x ways when another
	 * path splits the same step y ways where x != y) and calculate the split level for each step.
	 *
	 * @param workflow workflow definition
	 *
	 * @return key is step name and values are a list of the different splits found leading into the step
	 */
	private def getStepSplits(workflow: WorkflowDefinition) = {
		/* Get graph and make sure it's not cyclic */
		val graph = workflow.toGraph
		val cycles = graph.findCycle
		if (cycles.isDefined) throw InvalidWorkflow("Workflow can not be cyclic.  Workflow "
		  + workflow.name + " contains " + cycles.get.toString)

		/**
		 * Get split data about step and descendants.
		 *
	 	 * @param step step we start from
		 * @param soFar split data so far keyed by step
		 * @param curSplit split data for step we're starting with
		 *
		 * @return map of split data updated with data for the requested step and steps it has a path to
		 */
		def findSplitNumbers(step: graph.NodeT, soFar: Map[String, List[Array[SplitInfo]]],
		                     curSplit: Stack[SplitInfo]) : Map[String, List[Array[SplitInfo]]] = {
			val stepsMap = getStepToSplitsFound(step.toString(), curSplit, soFar)
			step.outgoing.foldLeft(stepsMap)((total, edgeOut) => {
				val splitStack = getNewSplitStack(edgeOut, curSplit)
				findSplitNumbers(edgeOut._2, total, splitStack)
			})
		}

		/**
		 * Update stack of split based on new edge.  If the edge is a split add a split to the stack, if a join then
		 * pop off one split from stack, if neither then leave the stack untouched.
		 *
		 * @param edge flow between steps
		 * @param curSplit current state of split stack
		 *
		 * @return update state of split stack leading to output from edge
		 */
		def getNewSplitStack(edge: graph.EdgeT, curSplit: Stack[SplitInfo]) : Stack[SplitInfo] = {
			val join = getJoinSpec(edge._2, edge.label.toString)
			val split = getSplitSpec(edge._1, edge.label.toString)
			assert(!(join.isDefined && split.isDefined), "Both join and split defined for " + edge)
			if (split.isDefined) {
				curSplit.push(SplitInfo(edge.label.toString, split.get))
			} else if (join.isDefined) {
				if (curSplit.isEmpty) throw InvalidWorkflow("Join specified on path " + edge + " not previously split")
				curSplit.pop
			} else {
				curSplit
			}
		}

		/**
		 * Get any split spec on the specified flow from a specified step.
		 *
	 	 * @param step name of step
		 * @param flow name of flow
		 *
		 * @return if flow is a split then the split specification is returned
		 */
		def getSplitSpec(step: String, flow: String) = {
			workflow.steps(step).outputs(flow) match {
				case SplitOutputStreamFile(_, _, _, _, splitSpec, _, _) => Some(splitSpec)
				case _ => None
			}
		}

		/**
		 * Get any join spec on the specified flow to a specified step.
		 *
		 * @param step name of step
		 * @param flow name of flow
		 *
		 * @return if flow is a join then the join specification is returned
		 */
		def getJoinSpec(step: String, flow: String) =
			workflow.steps(step).inputs(flow) match {
				case JoinInputStreamDataFlow(_, _, joinSpec) => Some(joinSpec)
				case _ => None
			}


		/**
		 * Update entry in map with split data for step.
		 *
		 * @param step step to be updated
		 * @param newSplit split data found for step
		 * @param splitsFound map to be updated
		 *
		 * @return map updated with new split data for step
		 */
		def getStepToSplitsFound(step: String, newSplit: Stack[SplitInfo],
		                         splitsFound: Map[String, List[Array[SplitInfo]]]) = {
			val splitsForStep = splitsFound.getOrElse(step, List.empty[Array[SplitInfo]])
			splitsFound + (step -> (newSplit.reverse.toArray +: splitsForStep))
		}

		/* Go get starting nodes (no edges are directed into them) */
		val starts = graph.nodes.filter(_.diPredecessors.isEmpty)

		/* Get split levels for steps */
		val stepsToSplits = starts.foldLeft(Map.empty[String, List[Array[SplitInfo]]])((soFar, start) => {
			findSplitNumbers(start, soFar, new Stack[SplitInfo])
		})

		/* Check that the splitting is legit */
		stepsToSplits.foreach((entry) => {
			val (step, splits) = entry
			checkStepInstances(step, splits)
		})

		/* Return split step information */
		stepsToSplits
	}

}
