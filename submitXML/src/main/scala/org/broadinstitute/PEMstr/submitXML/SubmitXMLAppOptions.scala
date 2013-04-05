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

package org.broadinstitute.PEMstr.submitXML

import collection.immutable.HashMap
import org.broadinstitute.PEMstr.common.AppOptions

/**
 * @author Nathaniel Novod
 * Date: 12/20/12
 * Time: 10:01 PM
 *
 * Class used to parse command line arguments and save the results
 *
 * @param args command line arguments
 */
class SubmitXMLAppOptions(args: Array[String]) extends AppOptions(args) {

	/** Enumeration for command parsing states including options that have associated value */
	object CommandOptions extends Enumeration
	{
		val SWITCH, XML, RETRY, PEM_HOST, PEM_PORT, ERROR = Value
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
		"-xml" -> OptionsToSet(None, XML, "<xml file specification>", true, 10),
		"-host" -> OptionsToSet(None, PEM_HOST, "<Central PEM scheduler host name>", false, 30),
		"-port" -> OptionsToSet(None, PEM_PORT, "<Central PEM scheduler port number>", false, 40),
		"-retry" -> OptionsToSet(None, RETRY, "<# seconds interval between retries of workflow>", false, 50),
		"-help" -> OptionsToSet(None, ERROR, "(output this message and exit)", false, 170)
	)

	/**
	 * Command is valid if an XML file is specified
 	 * @return true if command is good
	 */
	def isCommandValid = parseResult.options.get(XML).isDefined && parseResult.parseState == SWITCH
	/**
	 * Get input file name
	 *
	 * @return xml file name specified
	 */
	def getXMLFile = parseResult.options.get(XML)
	/**
	 * Get retry time
	 *
	 * @return # of seconds to retry
	 */
	def getRetry = {
		val retry = parseResult.options.get(RETRY)
		if (retry.isDefined) Some(retry.get.toInt) else None
	}
	/**
	 * Get PEM host name
	 *
	 * @return optional host name
	 */
	def getPEMhost = parseResult.options.get(PEM_HOST)
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