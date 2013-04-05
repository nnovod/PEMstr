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

package org.broadinstitute.PEMstr.common

import collection.immutable.{Set, HashMap}
import annotation.tailrec

/**
 * @author Nathaniel Novod
 * Date: 12/24/12
 * Time: 6:25 PM
 *
 * Parse command arguments.  This class must be extended to specify the options specific to the application.
 */
abstract class AppOptions(args: Array[String]) {
	/**
	 * Values to say what state parsing is at.  Each subclass must define at least the two options SWITCH and ERROR.
	 */
	val commandOptions : Enumeration
	val SWITCH : commandOptions.Value
	val ERROR : commandOptions.Value

	/**
	 * Flags set for found option.
	 */
	val actionOptions : Enumeration

	/**
	 * Used to declare what to do for different command line arguments
	 *
	 * @param action ActionOptions to set
	 * @param nextState next parsing state
	 * @param help help string
	 * @param mandatory is argument mandatory?
	 * @param order order of option in help - note that order is within fellow mandatory or non-mandatory options
	 * @param multiArg multiple values allowed for option
	 */
	protected case class OptionsToSet(action: Option[actionOptions.Value],
	                                  nextState: commandOptions.Value,
	                                  help: String,
	                                  mandatory: Boolean,
	                                  order: Int,
	                                  multiArg: Boolean = false)

	/**
	 * Map of command options that must be set by subclass.  Keys are the option syntax and values say what to do
	 * with the option.
 	 */
	val optionValues : HashMap[String, OptionsToSet]

	/**
	 * Get the command syntax - this can be used for help messages etc.
	 *
	 * @return String containing command line syntax
	 */
	def cmdSyntax = {
		"Mandatory arguments (one or more of the following must be specified):\n" +
		  outputHelp(optionValues, mandatory = true) + "\n" +
		  "Optional arguments:\n" +
		  outputHelp(optionValues, mandatory = false)
	}

	/**
	 * Get an ordered list of options (one per line), with associated help
	 *
	 * @param values map of command line arguments to associated options
	 * @param mandatory retrieve mandatory (true) or optional (false) arguments only
	 * @return String containing ordered list of options
	 */
	private def outputHelp(values: HashMap[String, OptionsToSet], mandatory: Boolean) = {
		val helpList : List[String] = for (value <- values.filter((entry:(String,  OptionsToSet)) => entry._2.mandatory == mandatory)
		  .toList.sortBy[Int]((e: (String,  OptionsToSet))=> e._2.order)) yield (value._1 + " " + value._2.help)
		helpList.mkString("\n")
	}

	/**
	 * Used to track state of command options found
	 *
	 * @param parseState current parsing state
	 * @param actions actions found for command
	 * @param options options and associated string values found for command
	 * @param badSyntax if set then bad syntax found
	 * @param multiArg if true then multiple arguments can be found for option found
	 */
	protected case class OptionsFound(parseState: commandOptions.Value,
	                                  actions: Set[actionOptions.Value],
	                                  options: HashMap[commandOptions.Value, String], badSyntax: Option[String],
	                                  multiArg: Boolean = false)

	/**
	 * Result of parsing complete command - must be lazy to make sure it's not setup during construction when this
	 * super class is being constructed before it's subclass has set abstract values needed.
	 */
	protected lazy val parseResult = getOption(args, 0,
		OptionsFound(SWITCH, Set.empty[actionOptions.Value], HashMap.empty[commandOptions.Value, String], None))

	/**
	 * Parse options. Recursive till all options are parsed
	 *
	 * @param args command line arguments
	 * @param curArg index into args to next argument to parse
	 * @param state state of parsing done so far
	 * @return state of parsing done
	 */
	@tailrec
	private def getOption(args: Array[String], curArg: Int, state: OptionsFound) : OptionsFound = {
		if (curArg >= args.size) state else {
			if (state.multiArg) {
				val (nextArg, valFound) = getMultiArg(args, curArg)
				getOption(args, nextArg,
					OptionsFound(SWITCH, state.actions, state.options + (state.parseState -> valFound), None))
			} else {
			// Recurse to go to next argument, setting updated state based on argument currently being parsed
				getOption(args, curArg+1,
					state.parseState match {
						case SWITCH =>
						{
							val entry = optionValues.find(
								(e:(String,  OptionsToSet)) => e._1.compareToIgnoreCase(args(curArg)) == 0)
							if (entry.isDefined) {
								// Found valid argument - now see if "action" argument or one requiring a value
								val opt = entry.get._2
								OptionsFound(opt.nextState,
									if (opt.action.isDefined) state.actions + opt.action.get else
										state.actions, state.options, None, opt.multiArg)
							} else {
								// Syntax error
								OptionsFound(ERROR, state.actions, state.options, Some(args(curArg)))
							}
						}
						case ERROR => state
						case _ => // All other states are retrieving value to associate with previous argument
							OptionsFound(SWITCH, state.actions,
								state.options + (state.parseState -> args(curArg)), None)
					}
				)
			}
		}
	}

	/**
	 * Get multiple argument value.
	 *
	 * @param args command line arguments
	 * @param curArg index into args to next argument to parse
	 *
	 * @return (updated Index, multi-argument value found)
	 */
	private def getMultiArg(args: Array[String], curArg: Int) = {

		/**
		 * Recursive routine to build up string
		 *
 		 * @param args command line arguments
		 * @param curArg index into args to next argument to parse
		 * @param soFar multiple argument string built up so far
		 *
		 * @return (updated arg index, multiple arguments found separated by a space)
		 */
		def getArgs(args: Array[String], curArg: Int, soFar: String) : (Int, String) =
			if (curArg >= args.size || args(curArg).startsWith("-")) (curArg, soFar) else
				getArgs(args, curArg + 1, soFar + " " + args(curArg))

		getArgs(args, curArg + 1, args(curArg))
	}

	/**
	 * Is the command valid?
	 *
 	 * @return true if command is good
	 */
	def isCommandValid : Boolean

	/**
	 * Report if there are any errors
	 *
	 * @return true if errors reported (command is invalid)
 	 */
	def reportError(out: (String) => Unit = println) = {
		if (!isCommandValid) {
			if (getCommandError != None)
				out("Invalid option: " + getCommandError.get)
			out(cmdSyntax)
		}
		!isCommandValid
	}

	/**
	 * @return optional invalid argument found
	 */
	def getCommandError = parseResult.badSyntax

	/**
	 * Parse a datetime of the format yyyy-mm-ddThh:mm:ss to see if it is valid (datetime can end before "T", ":" or "-")
	 *
	 * @return true if valid datetime, otherwise false
	 */
	def isDatetimeValid(datetime: String) = {
		val regexp = """^(\d{4})((-\d{2})|$)((-\d{2})|$)((T\d{2})|$)((:\d{2})|$)((:\d{2})|$)$""".r
		regexp.findFirstMatchIn(datetime).isDefined
	}

}
