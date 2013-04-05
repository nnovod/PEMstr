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

package org.broadinstitute.PEMstr.splitApp

import collection.immutable.HashMap
import org.broadinstitute.PEMstr.common.AppOptions

/**
 * @author Nathaniel Novod
 * Date: 2/28/13
 * Time: 1:45 PM
 */
class SplitAppOptions(args: Array[String]) extends AppOptions(args) {

	/** Enumeration for command parsing states including options that have associated value */
	object CommandOptions extends Enumeration
	{
		val SWITCH, INPUT, OUTPUT, ERROR = Value
	}

	import CommandOptions._

	val commandOptions = CommandOptions
	val SWITCH = commandOptions.SWITCH
	val ERROR = commandOptions.ERROR

	object ActionOptions extends Enumeration
	val actionOptions = ActionOptions

	/**
	 * This is the heart of the command line syntax.  For each option there is the option syntax and an associated OptionsToSet object
	 * used to determine what to do when the option is found.
	 */
	val optionValues = HashMap[String, OptionsToSet] (
		"-input" -> OptionsToSet(None, INPUT, "<names of input files>", true, 10, true),
		"-output" -> OptionsToSet(None, OUTPUT, "<names of output files>", true, 20, true),
		"-help" -> OptionsToSet(None, ERROR, "(output this message and exit)", false, 170)
	)

	/**
	 * Command is valid if inputs and outputs specified
 	 * @return true if command is good
	 */
	def isCommandValid = parseResult.options.get(OUTPUT).isDefined &&
		parseResult.options.get(INPUT).isDefined && parseResult.parseState == SWITCH

	/**
	 * Get input file names
	 *
	 * @return space separated input file names specified
	 */
	def getInputFile = parseResult.options.get(INPUT)

	/**
	 * Get output file names
	 *
	 * @return space separated output file names specified
	 */
	def getOutputFile = parseResult.options.get(OUTPUT)
}
