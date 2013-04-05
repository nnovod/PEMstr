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

import org.broadinstitute.PEMstr.common.AppOptions
import collection.immutable.HashMap

/**
 * @author Nathaniel Novod
 * Date: 12/30/12
 * Time: 10:41 PM
 *
 * Parse options specified in the command line arguments
 *
 * @param args command line arguments
 */
class CheckStateAppOptions (args: Array[String]) extends AppOptions(args) {

	/** Enumeration for command parsing states including options that have associated value */
	object CommandOptions extends Enumeration
	{
		val SWITCH, WORKFLOW_STATUS, WORKFLOW_ABORT, PEM_HOST, PEM_PORT, ERROR = Value
	}
	import CommandOptions._

	val commandOptions = CommandOptions
	val SWITCH = commandOptions.SWITCH
	val ERROR = commandOptions.ERROR

	object ActionOptions extends Enumeration {
		val WORKFLOW_NAMES, WORKFLOW_STATES = Value
	}
	import ActionOptions._

	val actionOptions = ActionOptions

	/**
	 * This is the heart of the command line syntax.  For each option there is the option syntax and an associated
	 * OptionsToSet object used to determine what to do when the option is found.
	 */
	val optionValues = HashMap[String, OptionsToSet] (
		"-workflows" -> OptionsToSet(Some(WORKFLOW_NAMES), SWITCH,
			"(Outputs a list of all workflow names)", true, 10),
		"-allStates" -> OptionsToSet(Some(WORKFLOW_STATES), SWITCH,
			"(Outputs the state of all workflows)", true, 20),
		"-workflowState" -> OptionsToSet(None, WORKFLOW_STATUS,
			"<Name of workflow to dump the state of>", true, 25),
		"-workflowAbort" -> OptionsToSet(None, WORKFLOW_ABORT,
			"<Name of workflow to abort>", true, 27),
		"-host" -> OptionsToSet(None, PEM_HOST, "<Central PEM scheduler host name>", false, 30),
		"-port" -> OptionsToSet(None, PEM_PORT, "<Central PEM scheduler port number>", false, 40),
		"-help" -> OptionsToSet(None, ERROR, "(output this message and exit)", false, 170)
	)

	/**
	 * Command is valid if at least one request is specified.
 	 * @return true if command is good
	 */
	def isCommandValid = parseResult.parseState == SWITCH &&
	  (parseResult.actions(WORKFLOW_NAMES) || parseResult.actions(WORKFLOW_STATES) ||
	    parseResult.options.get(WORKFLOW_ABORT).isDefined || parseResult.options.get(WORKFLOW_STATUS).isDefined)

	/**
	 * Dump of workflow names wanted?
	 * @return true if dump of names wanted
	 */
	def isNamesDump = parseResult.actions(WORKFLOW_NAMES)

	/**
	 * Dump of workflow states wanted?
	 * @return true if dump of all workflow states wanted
	 */
	def isStatusDump = parseResult.actions(WORKFLOW_STATES)

	/**
	 * Get workflow to abort
	 * @return optional workflow to abort
	 */
	def getWorkflowAbort = parseResult.options.get(WORKFLOW_ABORT)

	/**
	 * Get workflow to get status for
	 * @return optional workflow to get status for
	 */
	def getWorkflowStatus = parseResult.options.get(WORKFLOW_STATUS)

	/**
	 * Get central scheduler host name
	 * @return optional host name
	 */
	def getPEMhost = parseResult.options.get(PEM_HOST)

	/**
	 * Get central scheduler port #
	 * @return optional port #
	 */
	def getPEMport = {
		val port = parseResult.options.get(PEM_PORT)
		if (port.isDefined) Some(port.get.toInt) else None
	}

}
