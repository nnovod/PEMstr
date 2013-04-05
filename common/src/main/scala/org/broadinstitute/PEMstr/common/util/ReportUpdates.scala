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

package org.broadinstitute.PEMstr.common.util

import org.broadinstitute.PEMstr.common.util.Util._
import scala.Some

/**
 * @author Nathaniel Novod
 * Date: 1/21/13
 * Time: 11:47 AM
 *
 * Trait to report on an items usage.  Reports can be made periodically or for the current state.  Typically a report of
 * the current state is only done when usage is complete.
 */
trait ReportUpdates {
	/**
	 * By default what we call the operations taking place
 	 */
	private val operationStr = "request"
	/**
	 * Amount of data used
	 */
	private var dataSent = BigInt(0)
	/**
	 * How frequently to do periodic reports
	 */
	protected val reportDif : BigInt
	/**
	 * When the next periodic report should take place
	 */
	protected var nextReport : BigInt
	/**
	 * How many items have been reported used
	 */
	private var numReports = BigInt(0)
	/**
	 * Type of item being reported on
	 */
	protected val reportType: String

	/**
	 * Make string that reports on current item usage.
	 *
 	 * @param operation what operations taking place are called
	 *
	 * @return "# <type>(s) for # <operation>(s) (<averageSummary>)"
	 */
	private def curItems(operation: String) =
		plural(dataSent, reportType) + " for " + plural(numReports, operation) + " " + getStat(operation)

	/**
	 * Get average summary for report.  Either average number of items used per operation (if the # of items used is
	 * greater than the number of operations) or the % of operations requiring a new item (if the # of items used is
	 * less than or equal to the number of operations)
	 *
	 * @param operation what operations taking place are called
	 *
 	 * @return "averaging # <type>(s) per operation" or "(<#>%)"
	 */
	private def getStat(operation: String) =
		if (dataSent > numReports) {
			val avg = if (numReports == 0) BigInt(0) else dataSent/numReports
			"averaging " + plural(avg, reportType) + " per " + operation
		} else {
			val pct = if (numReports == 0) 0.0F else (dataSent.toFloat/numReports.toFloat)*100F
			"(" + (pct).formatted("%.2f") + "%)"
		}

	/**
	 * Decide if the amount of items added is worth reporting - we report every time we go over reportDif # of items
	 * past the last report.
	 *
 	 * @param intro text to set as intro in reporting message
	 * @param newItems # of new items added
	 *
	 * @return if the new threshold is reached a report of the amount of data used
	 */
	def dataReport(intro: String, newItems: Long, operation: String = operationStr) = {
		dataSent += newItems
		numReports += 1
		if (dataSent >= nextReport) {
			nextReport += reportDif
			Some(intro + curItems(operation))
		} else None
	}

	/**
	 * Return string containing report with # of items used
	 *
	 * @param intro text to set as intro in reporting message
	 *
	 * @return report of # of items used
	 */
	def getCurrentReport(intro: String, operation: String = operationStr) = intro + curItems(operation)

}
