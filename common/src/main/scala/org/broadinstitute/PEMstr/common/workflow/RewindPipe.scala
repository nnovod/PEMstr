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

import scala.util.matching.Regex
import org.broadinstitute.PEMstr.common.util.Util._
import java.nio.ByteBuffer
import scala.Some

/**
 * @author Nathaniel Novod
 * Date: 1/5/13
 * Time: 9:07 AM
 *
 * Handles a "rewind" of data.  For some application the start of data must be sent out twice - once for
 * some preliminary work (e.g., checking the format of the file) and then "rewound" back to the start
 * of the data and sent out again for the main application.
 *
 * @param records # of records to send out before rewinding back to the start
 * @param delay # of seconds to wait before doing the rewind back to the start
 * @param reopenFile true if the file should be closed and reopened when doing a rewind
 */
sealed abstract class RewindPipe(val records: Int, val delay: Int,
                                 val reopenFile: Boolean) extends Serializable {
	/* Values to be set by inheritors: type of data and regular expression to use to recognize records */
	val dataType: String
	val regexp: Regex

	/**
	 * See if we can find a match for a wanted record.
	 *
 	 * @param str String to look into for wanted records
	 * @param matchesLeft # of matches more we want to find
	 * @param matchesSoFar # of matches we've found so far (used for recursion)
	 *
	 * @return Optionally the length of the match found - None if the wanted match is not found
w	 */
	private def getMatchLen(str: String, matchesLeft: Int, matchesSoFar: Int = 0) : Option[Int] = {
		if (matchesLeft == 0) Some(matchesSoFar) else {
			regexp.findFirstIn(str) match {
				case Some(s) => getMatchLen(str.substring(s.length, str.length),
					matchesLeft - 1, matchesSoFar + s.length)
				case None => None
			}
		}
	}

	/**
	 * Get the length of the data to be "rewound".
	 *
 	 * @param data data read that hopefully contains rewind data
	 *
	 * @return Optionally the length of the match found - None if the wanted match is not found
	 */
	def getRewindLen(data: Array[ByteBuffer]) : Option[Int] = {
		// @TODO need to take CharSet option for rewind?  byteBuffersToString defaults to machine default CharSet
		val dataStr = byteBuffersToString(data)
		getMatchLen(dataStr, records)
	}
}

/**
 * Extension of RewindPipe for fastq data with the format:
 * \@SEQ_ID
 * GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
 * +
 * !''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
 *
 * @param numRecords # of records to send out before rewinding back to the start
 * @param secondsDelay # of seconds to wait before doing the rewind back to the start
 * @param reopen true if the file should be closed and reopened when doing a rewind
 */
case class FastqRewind(private val numRecords: Int, private val secondsDelay: Int,
                       private val reopen: Boolean)
  extends RewindPipe(numRecords, secondsDelay, reopen) {
	val regexp = """(^@.*\n.*\n\+.*\n.*\n)""".r  // 4 lines - the 1st starts with "@", the 3rd with "+"
	val dataType = "fastq"
}

/**
 * Matching object for allocation factory
 */
object RewindPipe {
	/**
	 * Factor method to get the proper RewindPipe
	 * @param rewindType type of rewind (e.g., fastq)
	 * @param recs # of records to rewind
	 * @param secondsDelay # of seconds to wait before doing the rewind back to the start
	 * @param reopen true if the file should be closed and reopened when doing a rewind
	 *
	 * @return proper subclass of RewindPipe
	 */
	def getRewindClass(rewindType: String, recs: Int,
	                   secondsDelay: Option[Int], reopen: Option[Boolean]) : RewindPipe = {
		val delay = secondsDelay.getOrElse(3)
		val reopenFile = reopen.getOrElse(true)
		rewindType.toLowerCase match {
			case "fastq" => FastqRewind(recs, delay, reopenFile)
			case _ => throw new IllegalArgumentException("Invalid rewind type: " + rewindType)
		}
	}
}
