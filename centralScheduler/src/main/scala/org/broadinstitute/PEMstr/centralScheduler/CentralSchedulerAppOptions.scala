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

import collection.immutable.HashMap
import org.broadinstitute.PEMstr.common.AppOptions


/**
 * @author Nathaniel Novod
 * Date: 12/24/12
 * Time: 8:40 PM
 *
 * Parses the command line arguments for the central scheduler.
 *
 * @param args command line arguments
 */
class CentralSchedulerAppOptions(args: Array[String]) extends AppOptions(args) {

	/** Enumeration for command parsing states including options that have associated value */
	object CommandOptions extends Enumeration
	{
		val SWITCH, PEM_PORT, ERROR = Value
	}
	import CommandOptions._

	val commandOptions = CommandOptions
	val SWITCH = commandOptions.SWITCH
	val ERROR = commandOptions.ERROR

	object ActionOptions extends Enumeration
	val actionOptions = ActionOptions

	/**
	 * This is the heart of the command line syntax.  For each option there is the option syntax and an associated
	 * OptionsToSet object used to determine what to do when the option is found.
	 */
	val optionValues = HashMap[String, OptionsToSet] (
		"-port" -> OptionsToSet(None, PEM_PORT, "<Central PEM scheduler port number>", false, 40),
		"-help" -> OptionsToSet(None, ERROR, "(output this message and exit)", false, 170)
	)

	/**
 	 * @return true if command is good
	 */
	def isCommandValid = parseResult.parseState == SWITCH

	/**
	 * Get PEM port #
	 *
	 * @return optional port #
	 */
	def getPEMport = {
		val port = parseResult.options.get(PEM_PORT)
		if (port.isDefined) Some(port.get.toInt) else None
	}


}
