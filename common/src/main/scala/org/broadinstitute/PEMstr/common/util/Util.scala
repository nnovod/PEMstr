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

import java.text.SimpleDateFormat
import java.nio.charset.Charset.defaultCharset
import java.nio.ByteBuffer
import java.nio.charset.Charset
import akka.util.ByteString

/**
 * @author Nathaniel Novod
 * Date: 10/4/12
 * Time: 2:53 PM
 *
 * A few generally useful static methods
 */

object Util {
	/**
	 * Make proper pluralization of word after number
	 *
	 * @param num number of items
	 * @param what what item is
	 * @return "<num> <what>" plus s at end if num is not one
	 */
	def plural(num: Int, what: String) = num.toString + " " + what + (if (num != 1) "s" else "")

	/**
	 * Make proper pluralization of word after number
	 *
	 * @param num number of items
	 * @param what what item is
	 * @return "<num> <what>" plus s at end if num is not one
	 */
	def plural(num: BigInt, what: String) = num.toString + " " + what + (if (num != BigInt(1)) "s" else "")

	/**
	 * Get the current time
	 *
	 * @return current time as number of milliseconds since "the epoch" (January 1, 1970, 00:00:00 GMT)
	 */
	def getTime = (new java.util.Date()).getTime

	/**
	 * Get a time string ID with millisecond precision
	 *
	 * @return string in format yyyyMMddHHmmssSSS
	 */
	def getTimeID(time: Long = getTime) = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new java.util.Date(time))

	/**
	 * Convert a ByteBuffer to a java String
	 *
	 * @param data bytes to convert
	 * @param charset character set to use for conversion (defaults to OS default character set)
	 *
	 * @return converted character string
	 */
	def byteBuffersToString(data: Array[ByteBuffer], charset: String = defaultCharset().displayName()) = {
		def sizeOfData = data.foldLeft(0)((size, next) => size + next.limit)
		val sb = new StringBuilder(sizeOfData)
		data.foreach{(entry) =>
			val charSet = Charset.forName(charset)
			val decoder = charSet.newDecoder
			/* Important to use duplicate ByteBuffer because position is altered */
			sb.append(decoder.decode(entry.duplicate()).toString)
		}
		sb.toString()
	}
}