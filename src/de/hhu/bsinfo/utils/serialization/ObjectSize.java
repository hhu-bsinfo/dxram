
package de.hhu.bsinfo.utils.serialization;

/**
 * Interface to define the size of an object for importing/exporting.
 *
 * @author Stefan Nothaas 17.12.15 <stefan.nothaas@hhu.de>
 */
public interface ObjectSize {
	/**
	 * Get the size of the object.
	 * This function has to return the sum in bytes of all data
	 * getting exported. Make sure this value is correct and matches
	 * the amount of data.
	 * Also make sure to first check if the object has a dynamic
	 * size. This might have influence on the value returned here
	 * i.e. it can change depending on the data stored in the object.
	 *
	 * @return Size of the object in bytes.
	 */
	int sizeofObject();
}
