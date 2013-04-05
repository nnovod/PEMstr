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

/**
 * @author Nathaniel Novod
 * Date: 12/14/12
 * Time: 5:48 PM
 *
 * Class to track when something is set for one or more entries.  A map is setup, typically keyed by a flow or step
 * name, allowing a value to be set for each entry.  When all the values are set then all the items are considered
 * complete.  Note that the values can only be set once and only for the set of possible keys given when the map is
 * initialized.  Once set a value can never be unset or reset.  This is done because we are typically tracking
 * information saved in one message to be retrieved in a following message and having the settings reset is
 * considered a bug.
 *
 * @param possibleKeys possible key values
 * @tparam K type of key for map (e.g., String if stream names are used)
 * @tparam T type of data for values in map
 */
class ValueMapTracker[K, T: Manifest](possibleKeys: List[K]) {
	/**
	 * Map of status values
	 */
	protected val status = collection.mutable.Map.empty[K, T]

	/**
	 * Set a status entry
	 *
	 * @param entryKey key to entry to set
	 */
	def update(entryKey: K, value: T) {
		if (possibleKeys.find(_ == entryKey).isEmpty)
			throw new Exception("attempt to set key " + entryKey + " not in possible set")
		if (isSet(entryKey)) throw new Exception("value already set for " + entryKey)
		status += entryKey -> value
	}

	/**
	 * Retrieve value for entry - value assumed to be already set
	 *
 	 * @param entryKey key to look for
	 * @return entry value
	 */
	def apply(entryKey: K) = status(entryKey)

	/**
	 * Check if status has been set.
	 *
	 * @param entryKey key to check
	 *
	 * @return true if status is set
	 */
	def isSet(entryKey: K) = status.get(entryKey).isDefined

	/**
	 * Check if all is set.
	 *
	 * @return true if all the entries are set
	 */
	def isAllSet = status.size == possibleKeys.size

	/**
	 * Call method for each key/value tuple
	 *
 	 * @param f method to call
	 */
	def foreach(f: ((K, T)) => Unit) { status.foreach(f) }

	/**
	 * Get keys for values set
	 *
	 * @return keys set
 	 */
	def keys = status.keys

	/**
	 * Get values set
	 *
	 * @return values set
	 */
	def values = status.values
}

/**
 * Extension of ValueMapTracker just used to track if something is setup.
 *
 * @param possibleKeys possible key values
 */
class StartMapTracker(possibleKeys: List[String]) extends ValueMapTracker[String, Boolean](possibleKeys) {
	/**
	 * Retrieve value for entry - value assumed to be already set
	 *
 	 * @param entryKey key to look for
	 * @return entry value
	 */
	override def apply(entryKey: String) = status.get(entryKey).isDefined && status(entryKey)

	/**
	 * Set a status entry
	 *
	 * @param entryKey key to entry to set
	 */
	def set(entryKey: String) { update(entryKey, true) }
}

/**
 * Used to track a single item that is set once.
 *
 * @tparam T value type
 */
class ValueTracker[T: Manifest] {
	/**
	 * Current status - should be set to Some(value) once and then never touched
	 */
	private var status : Option[T] = None

	/**
	 * Scala standard apply method to allow retrieval of values without need to use method name.
	 * It is assumed that any entry being fetched has already been set.
	 *
	 * @return value set for status entry
	 */
	def apply(): T = {
		if (!isSet) throw new Exception("value being retrieved not set")
		status.get
	}

	/**
	 * Scala standard update method to allow setting of values without need to use method name.
	 *
	 * @param v value to set entry to
	 */
	def update(v: T) {
		if (isSet) throw new Exception("value already set for ValueTracker")
		status = Some(v)
	}

	/**
	 * Check if set.
	 *
	 * @return true if status is complete
	 */
	def isSet : Boolean = status.isDefined
}

/**
 * Used to track if a single item has started.
 */
class StartTracker {
	/**
	 * Current status - should be set to true once and then never touched
	 */
	private var status: Boolean = false

	/**
	 * Set as complete
	 */
	def set() {
		if (isSet) throw new Exception("value already set for StartTracker")
		status = true
	}

	/**
	 * Check if set.
	 *
	 * @return true if status is complete
	 */
	def isSet : Boolean = status
}
