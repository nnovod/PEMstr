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

import scala.util.matching.Regex
import util.Util.getTimeID
import workflow.WorkflowDefinitions.StepDefinition
import workflow.WorkflowMessages.WorkflowID

/**
 * @author Nathaniel Novod
 * Date: 12/26/12
 * Time: 6:08 PM
 *
 * Do matching and replacing of tokens (variable and stream references) in strings.
 */
object StepCommandArgTokens {

	/**
	 * Name of regular expression group containing token type
	 */
	private val stepArgTokenTypeGroup = "tokenType"
	/**
	 * Name of regular expression group containing file name
	 */
	private val stepArgTokenValueGroup = "tokenValue"
	/**
	 * String prefix for stream tag in command argument
 	 */
	private val stepArgStreamTokenType = "stream"
	/**
	 * String prefix for variable tag in command argument
 	 */
	private val stepArgVariableTokenType = "variable"
	/**
	 * Partial regular expression for token value
	 */
	private val tokenValR = """([^}]+)"""
	/**
	 * Regular expression to find tokens to be replaced in command argument
	 */
	private val stepArgTokensR = new Regex("""\{(""" +
		stepArgStreamTokenType + "|" + stepArgVariableTokenType +
		""")=""" + tokenValR + """\}""",
		stepArgTokenTypeGroup, stepArgTokenValueGroup)
	/**
	 * Standard tokens within variables that are substituted with set values.  These names are used within a
	 * variable surrounding by open/close brackets {}
	 * For example a variable value could be defined as Hi{step}number{stepInstance}
	 *
	 * The names that can be used are:
	 * step - to get the original step name
	 * stepInstance - to get the step instance number (a blank string if the step does not have multiple instances)
	 * workflow - name of the workflow
	 * workflowTime - a text representation of the time the workflow started in the form yyyymmddhhmmssmmm, for
	 * example 20130307160043227 for Mar. 7, 2013 4:00:43.227 PM
 	 */
	private val tokenVariableStep = "step"
	private val tokenVariableStepInstance = "stepInstance"
	private val tokenVariableWorkflow = "workflow"
	private val tokenVariableWorkflowTime = "workflowTime"
	/* Map of system variable names to function to use to evaluate value at runtime */
	private val sysVarsEval = Map[String, (StepDefinition, WorkflowID) => String](
		tokenVariableStep -> ((s, _) => s.baseName),
		tokenVariableStepInstance -> ((s, _) =>
			s.instance match {
				case Some(i) => i.toString
				case None => ""
			}),
		tokenVariableWorkflow -> ((_, w) => w.name),
		tokenVariableWorkflowTime -> ((_, w) => getTimeID(w.timeID))
	)
	/**
	 * Name of regular expression group containing token variable type
	 */
	private val tokenVariableTypeGroup = "tokenArg"
	/**
	 * Regular expression to parse variables in tokens
 	 */
	private val tokenVariableR = new Regex("""\{""" + tokenValR + """\}""", tokenVariableTypeGroup)

	/**
	 * Replace tokens in a string with the assigned values.
	 *
 	 * @param input string to look in for tokens to replace
	 * @param replaceStream callback to replace stream token value
	 * @param replaceVariable callback to replace variable token value
	 *
	 * @return string with tokens replaced by their values
	 */
	private def replaceAllTokensIn(input: String, replaceStream: Option[(String) => String],
	                               replaceVariable: Option[(String) => String]) = {

		def tokenReplacement(replacement: Option[(String) => String], tokenType: String, tokenValue: String) = {
			if (replacement.isDefined) replacement.get(tokenValue) else
				tokenType + "={" + tokenValue + "}"
		}

		stepArgTokensR.replaceAllIn(input,
			(m) => {
				val mValue = m.group(stepArgTokenValueGroup)
				m.group(stepArgTokenTypeGroup) match {
					case stream if stream == stepArgStreamTokenType => tokenReplacement(replaceStream, stream, mValue)
					case token if token == stepArgVariableTokenType => tokenReplacement(replaceVariable, token, mValue)
				}
			})
	}

	/**
	 * Replace stream tokens in a string with the assigned values.
	 *
 	 * @param input string to look in for tokens to replace
	 * @param replaceStream callback to replace stream token value
	 *
	 * @return string with tokens replaced by their values
	 */
	def replaceStreams(input: String, replaceStream: (String) => String) =
		replaceAllTokensIn(input, Some(replaceStream), None)

	/**
	 * Replace variable tokens in a string with the assigned values.
	 *
 	 * @param input string to look in for variables to replace
	 * @param replaceVariable callback to replace variable token value
	 *
	 * @return string with tokens replaced by their values
	 */
	def replaceVariables(input: String, replaceVariable: (String) => String) =
		replaceAllTokensIn(input, None, Some(replaceVariable))

	/**
	 * Replace tokens in a string with the assigned values.
	 *
 	 * @param input string to look in for tokens to replace
	 * @param replaceStream callback to replace stream token value
	 * @param replaceVariable callback to replace variable token value
	 *
	 * @return string with tokens replaced by their values
	 */
	def replaceStreamsAndVariables(input: String, replaceStream: (String) => String, replaceVariable: (String) => String) =
		replaceAllTokensIn(input, Some(replaceStream), Some(replaceVariable))

	/**
	 * Get the string value for a variable.  Variable values can include arbitrary text as well as
	 * "known variables" to retrieve a step name, step instance, workflow name or workflow time.  We replace
	 * the variables with their values.
	 *
	 * @param stepDef definition of step we're doing substitution for
	 * @param variables map of variable names to variable values
	 * @param wfID ID info for workflow we're doing substitution for
	 * @param variableName name of variable to get the value for
	 *
	 * @return token value, including substitutions for variable names
	 */
	def getVariableValue(stepDef: StepDefinition, variables: Map[String, String],
	                     wfID: WorkflowID, variableName: String) = {

		/**
		 * Recursive get variable value method to allow variables (expressed as {variableName}) within variable values.
		 *
 		 * @param newVar variable name to look for value of
		 * @param varsSoFar variable names found so far (to avoid infinite loop)
		 * @param vars map of variable names to variable values
		 *
		 * @return string with all variables replaced with variable values
		 */
		def getValue(newVar: String, varsSoFar: Set[String], vars: Map[String, String]) : String = {
			vars.get(newVar) match {
				case Some(varValue) =>
					tokenVariableR.replaceAllIn(varValue,
						(m)  => {
							val varFound = m.group(tokenVariableTypeGroup)
							/* Get contained variable's value unless we're in a loop of variable definitions */
							if (varsSoFar.contains(varFound)) "" else
								getValue(varFound, varsSoFar + varFound, vars)
						})
				case None => ""
			}
		}

		/* Get values for system values */
		val sysVars = sysVarsEval.map((entry) => {
			val (name, func) = entry
			name -> func(stepDef, wfID)
		})
		/* Call to replace variables found - note that user variable of same name as system variable takes precedence */
		getValue(variableName, Set.empty[String], sysVars ++ variables)
	}

	/**
	 * Look for matches in a string.
	 *
 	 * @param input string to look for stream/token matches
	 * @param matchStream optional callback when a stream match is found
	 * @param matchVariable optional callback when a variable match is found
	 */
	private def matchTokens(input: String, matchStream: Option[(String) => Unit],
	                        matchVariable: Option[(String, Boolean) => Unit]) {
		val matchIterator = stepArgTokensR.findAllIn(input)
		matchIterator.matchData.foreach((m) => {
			val tokenType = m.group(stepArgTokenTypeGroup)
			val tokenValue = m.group(stepArgTokenValueGroup)
			tokenType match {
				case s if (s == stepArgStreamTokenType) => if (matchStream.isDefined) matchStream.get(tokenValue)
				case s if (s == stepArgVariableTokenType) => if (matchVariable.isDefined) {
					matchVariable.get(tokenValue, sysVarsEval.keys.find(_ == tokenValue).isDefined)
				}
			}
		})
	}

	/**
	 * Match stream tokens in a string.
	 *
 	 * @param input string to look in for tokens to match
	 * @param matchStream callback to call when match found for stream token
	 *
	 */
	def matchStreams(input: String, matchStream: (String) => Unit) {
		matchTokens(input, Some(matchStream), None)
	}

	/**
	 * Match variable tokens in a string.
	 *
 	 * @param input string to look in for variables to match
	 * @param matchVariable callback to call when match for variable token found
	 *
	 */
	def matchVariables(input: String, matchVariable: (String, Boolean) => Unit) {
		matchTokens(input, None, Some(matchVariable))
	}

	/**
	 * Match tokens in a string.
	 *
 	 * @param input string to look in for tokens to match
	 * @param matchStream callback to call when match for stream token found
	 * @param matchVariable callback to call when match for variable token found
	 *
	 */
	def matchStreamsAndVariables(input: String, matchStream: (String) => Unit,
	                             matchVariable: (String, Boolean) => Unit) {
		matchTokens(input, Some(matchStream), Some(matchVariable))
	}

}
