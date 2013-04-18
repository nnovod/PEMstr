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

import java.io.{InputStreamReader,FileInputStream,FileWriter,BufferedWriter,BufferedReader}
import java.util.zip.InflaterInputStream

/**
 * @author nnovod
 */
object SplitApp {

	def main(args : Array[String]) {
		// Print out what we found on the command line
		if (args.length != 0) println("Options found on command line: " + args.mkString(" "))
		// Go parse the command
		val optionsFound = new SplitAppOptions(args)

		// If there was an error parsing the command line then report the error, output the correct syntax and exit
		if (!optionsFound.isCommandValid) {
			if (optionsFound.getCommandError != None)
				println("Invalid option: " + optionsFound.getCommandError.get)
			println(optionsFound.cmdSyntax)
		} else {
			val inputs = optionsFound.getInputFile.get.split(" ")
			val outputs = optionsFound.getOutputFile.get.split(" ")
			val writers = outputs.map((f) => new BufferedWriter(new FileWriter(f)))
			val splitLevel = writers.length
			for (input <- inputs) {
				val fileInput = new FileInputStream(input)
				val inputStream = if (input.endsWith(".gz")) new InflaterInputStream(fileInput) else fileInput
				val reader = new BufferedReader(new InputStreamReader(inputStream))
				if (splitLevel > 0) {

					def getNextRead = {
						(for (i <- 1 to 4) yield {
							val line = reader.readLine()
							if (line != null) line else ""
						}).toList
					}

					var nextWriter = 0
					var nextRead = getNextRead
					while (nextRead.forall(_.length != 0)) {
						nextRead.foreach((r) => {
							writers(nextWriter).write(r)
							writers(nextWriter).newLine()
						})
						nextRead = getNextRead
						nextWriter += 1
						if (nextWriter == writers.length) nextWriter = 0
					}
				}
				reader.close()
			}
			writers.foreach((w) => {
				w.flush()
				w.close()
			})
		}
	}
}
