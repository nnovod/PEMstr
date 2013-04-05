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
 * Date: 11/5/12
 * Time: 11:20 AM
 *
 * A few useful static methods for parsing XML
 */

import xml.{Node, NodeSeq}

object XMLUtil {

	/**
	 * Get subnode that should only be there once or not at all.  An exception is thrown if the tag is there
	 * multiple times.
	 *
	 * @param xml subnode list
	 *
	 * @return optional node found
	 */
	private def getSingleNode(xml: NodeSeq) = {
		if (xml.isEmpty) None else {
			if (xml.size != 1) throw new Exception(xml.head.label + " can only be specified once")
			Some(xml.head)
		}
	}

	/**
	 * Get tag value from XML
	 *
 	 * @param xml element containing tag
	 * @param tag tag name (note "@" must be at start of name if needed)
	 *
	 * @return option value of tag (None if empty string found or tag not found)
	 */
	def getXMLTag(xml: NodeSeq,  tag: String) : Option[String] = {
		getSingleNode(xml \ tag) match {
			case Some(node) => {
				val str = node.text
				if (str.isEmpty) None else Some(str)
			}
			case None => None
		}
	}

	/**
	 * Get string from XML
	 *
	 * @param xml containing string
	 *
	 * @return optional trimmed text from xml (None if input XML is empty)
	 */
	def getXMLstring(xml: NodeSeq): Option[String] = {
		getSingleNode(xml) match {
			case Some(node) => {
				val str = trimXMLstring(node)
				if (str.isEmpty) None else (Some(str))
			}
			case None => None
		}
	}

	/**
	 * Trim all leading and trailing white space (including other carriage control from other nodes) from XML content
	 *
	 * @param xml sequence of XML nodes
	 *
	 * @return trimmed text from xml
	 */
	def trimXMLstring(xml: Node, trimWith: String = ""): String = {
		xml.text.split("\n").map(_.trim).mkString(trimWith)
	}

	/**
	 * Get an integer from a string
	 *
	 * @param inp xml entry with integer value
	 *
	 * @return optional integer of converted string
	 */
	def getXMLint(inp: NodeSeq): Option[Int] = {
		getXMLstring(inp) match {
			case Some(s) => Some(s.toInt)
			case None => None
		}
	}

	/**
	 * Get boolean value from XML entry
	 * Date should be in format true or false
	 *
	 * @param inp xml entry with true/false value
	 *
	 * @return optional date value found
	 */
	def getXMLboolean(inp: NodeSeq): Option[Boolean] = {
		getXMLstring(inp) match {
			case Some(s) => Some(s.toBoolean)
			case None => None
		}
	}

}