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
 * Save and display workflow paths.  Steps are linked together via their data flows.  Note this trait must be
 * mixed in with WorkflowXML.
 */
trait WorkflowXMLPath {
	/**
	 * This trait must be mixed in with WorkflowXML
 	 */
	this: WorkflowXML =>

	/**
	 * Get all paths for a workflow starting with paths that have a clear head step (no data streaming to a step).
	 *
	 * @return All paths for workflow
 	 */
	def getAllPaths = {
		/**
		 * Get all paths including paths input as found so far plus any paths for steps not included in the paths
		 * found so far.
		 *
		 * @param pathsSoFar All paths found so far
		 *
		 * @return All paths for workflow
		 */
		def getAllPaths(pathsSoFar: Seq[WorkflowPathStart]) : Seq[WorkflowPathStart] = {
			val stepsSoFar = (for (path <- pathsSoFar) yield getPathSteps(path)).flatten.distinct
			val nextPath = workflow.steps.find((step) => !stepsSoFar.exists(_ == step.name))
			if (nextPath.isDefined) getAllPaths(pathsSoFar :+ rootPath(nextPath.get.name)) else pathsSoFar
		}
		val headPaths = for (head <- getHeads) yield rootPath(head)
		getAllPaths(headPaths)
	}

	/**
	 * Get the names of all the steps in a path
	 *
 	 * @param pathStart Start of path
	 *
	 * @return Names of steps found in the path
	 */
	def getPathSteps(pathStart: WorkflowPathStart) = {
		def getPathSteps(pathSteps: List[WorkflowPathStep]) : List[String] = {
			pathSteps match {
				case head :: Nil => head.toStep +: getPathSteps(head.next)
				/* There's a tail - that means there are alternate paths starting from this node */
				case head :: tail => head.toStep +: (getPathSteps(head.next) ++ getPathSteps(tail))
				case _ => List.empty[String]
			}
		}
		val names = pathStart.startStep +: getPathSteps(pathStart.toSteps)
		names.distinct
	}

	/**
	 * Get the name of all the steps that are "head" steps (i.e., they have no other steps that
	 * flow to them)
	 *
	 * @return Seq of head step names
	 */
	def getHeads = for (step <- workflow.steps if !workflow.flowsTo.contains(step.name)) yield step.name

	/**
	 * Get the graph from a step to all other steps flowed to starting with the input step.
	 *
	 * @param root Name of step to use as start of graph
	 *
	 * @return WorkflowPathStart containing graph to all steps reached from root
	 */
	def rootPath(root: String)  = {

		/**
		 * Get all paths from specified step.  We create a recursive structure that shows all steps
		 * that a step goes to starting with the initial step specified.
		 *
		 * @param step Name of step to find
		 * @param flows List of flows step is going to
		 * @param foundSoFar List of steps that have already been found in the path
		 *
		 * @return WorkflowPathStep for all complete paths that can be followed from specified step
		 */
		def getNextPath(step: String, flows: List[String],
		                foundSoFar: List[String] = List.empty[String]) : WorkflowPathStep = {
			/*
			 * We're looping if step seen before
			 * If looping then flag it and don't go down (what would be an infinite) path any more
			 */
			val isLooping = foundSoFar.contains(step)
			WorkflowPathStep(step, isLooping, flows,
				if (isLooping) List.empty[WorkflowPathStep] else {
					for (toStepInfo <- getNextStep(step).toList)
					yield getNextPath(toStepInfo._1, toStepInfo._2.toList, step +: foundSoFar)
				}
			)
		}


		/**
		 * Get steps that immediately follow specified step along with associated flows to following steps
		 *
		 * @param startingStep name of step that we want to find next level of steps for
		 *
		 * @return Map of next level of step names as keys pointing to a sequence of flows that go to the step
		 */
		def getNextStep(startingStep: String) = {
			/* Get list of flows from wanted entry */
			val fromEntry = workflow.flowsFrom.get(startingStep)
			/* If step is not origin for anyone then there are no entries in next level */
			if (fromEntry.isEmpty) Map.empty[String, Seq[String]] else {
				/*
				 * Go through Data entries with wanted step as origin (from)
				 * accumulating map with steps going to wanted step via the data entries
 				 */
				fromEntry.get.foldLeft(Map.empty[String,  Seq[String]])((flowsTo, flow) => {
					/* Go through all the steps the Data entry goes to */
					flow.flowsTo.foldLeft(flowsTo)((flowsToMap, toStep) => {
						/* Add flow name to list of flows going to following step */
						val toEntry = flowsToMap.getOrElse(toStep.stepTo, Seq.empty[String])
						val flowType = if (flow.isSplit) "(S)" else if (flow.isJoin) "(J)" else ""
						flowsToMap + (toStep.stepTo -> ((flow.name + flowType) +: toEntry))
					})
				})
			}
		}

		/* Return the WorkflowPathStart */
		WorkflowPathStart(root,
			for (toStepInfo <- getNextStep(root).toList)
			yield getNextPath(toStepInfo._1, toStepInfo._2.toList, List(root)))
	}

	/**
	 * Workflow path from a starting step
	 *
	 * @param startStep name of step starting path
	 * @param toSteps next level of steps the starting step flows to
	 */
	case class WorkflowPathStart(startStep: String, toSteps: List[WorkflowPathStep])

	/**
	 * Step in workflow path travelled to.  A WorkflowPathStep should always be pointed to from a previous
	 * step in either a WorkflowPathStart or another WorkflowPathStep.
	 *
	 * @param toStep name of step travelled to
	 * @param isLoop flag is this step already appears in the path
	 * @param flows list of names of data streams and files used to get to the step
	 * @param next following level of path containing steps travelled to from toStep
	 */
	case class WorkflowPathStep(toStep: String, isLoop: Boolean, flows: List[String], next: List[WorkflowPathStep])

	/**
	 * Output path in format: step --via flowlist--> step ...
	 * for the complete graph of a workflow.
	 * Note that if a loop occurs the loop is marked by putting parenthesis around the step name
	 * where the loop occurs and the path stops where the loop starts.
	 *
	 * @param wf complete path of workflow
	 * @param indent # of spaces to indent output.  Used via recursion to line up subpaths.  For example if
	 *        there are paths A --via F1--> B --via F2--> C and B --via F3--> D this will be output as:
	 *{{{
	 * A --via F1--> B --via F2--> C
	 *               B --via F3--> D
	 *}}}
	 *        since the B->D path will be considered a new path with an indent to line up with the
	 *        alternate path from B
	 * @param first flag to say if first path (if false then new line including in output)
	 *
	 * @return string with all paths that originate at the specified start
	 */
	def drawAllPaths(wf: WorkflowPathStart, indent: Int = 0, first: Boolean = true) : String = {
		val start = " "*indent + wf.startStep
		(if (first) "" else "\n") + start +
			(wf.toSteps match {
				case head :: Nil => drawAllPaths(head, start.length)
				/* There's a tail - that means there are alternate paths starting from this node */
				case head :: tail => drawAllPaths(head, start.length) +
				  drawAllPaths(WorkflowPathStart(wf.startStep, tail), indent, first = false)
				case _ => ""
			})
	}

	/**
	 * Output continuing path in format: step --via flowlist--> step ...
	 * the remaining graph of a workflow.
	 * Note that if a loop occurs the loop is marked by putting parenthesis around the step name
	 * where the loop occurs and the path stops where the loop starts.
	 *
	 * @param wf path of remaining workflow
	 * @param indent # of spaces to indent output.  (See alternate drawAllPaths)
	 *
	 * @return string with all paths from specified step
	 */
	private def drawAllPaths(wf: WorkflowPathStep, indent: Int) : String = {
		val viaStart = drawVia(wf.flows)
		val newIndent = indent + viaStart.length()
		val toStep = drawTo(wf.toStep, wf.isLoop)
		viaStart + toStep +
			(wf.next match {
				case head :: Nil => drawAllPaths(head, newIndent + toStep.length())
				/* There's a tail - that means there are alternate paths starting from this node */
				case head :: tail => drawAllPaths(head, newIndent + toStep.length()) +
					drawAllPaths(WorkflowPathStart(wf.toStep, tail), newIndent, first = false)
				case _ => ""
			})
	}

	/**
	 * Create String to represent flows between steps
	 *
	 * @param via list of data flows
	 *
	 * @return string in format " --via flow,flow,...--> "
	 */
	private def drawVia(via: List[String]) = " --via " + via.mkString(",") + "--> "

	/**
	 * Create String with to step name, in parenthesis if we've looped to the step
	 *
	 * @param to step name
	 * @param loop true if we've looped to step
	 *
	 * @return string with step name, surrounded by parenthesis if we're looping
	 */
	private def drawTo(to: String, loop: Boolean) = if (loop) "(" + to + ")" else to

}
