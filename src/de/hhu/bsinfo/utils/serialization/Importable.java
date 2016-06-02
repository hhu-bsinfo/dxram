
package de.hhu.bsinfo.utils.serialization;

/**
 * Interface defining an object which can be imported/de-serialized.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.12.15
 */
public interface Importable extends ObjectSize {
	/**
	 * Import/De-serialize this object using the provided importer (target).
	 * Don't call this explicitly. Use an Importer which is calling this
	 * method implicitly.
	 * @param p_importer
	 *            Target to import/de-serialize the object from.
	 * @param p_size
	 *            Amount of bytes available for importing. This does not have
	 *            to match the size of the object, it can be more depending on
	 *            the implementation of the importer used. Make sure that this
	 *            size does at least match your targeting object size. If not
	 *            either cut down to match the size or abort importing.
	 * @return Number of bytes read from the importer or -1 if an error occurred and importing was aborted.
	 */
	int importObject(final Importer p_importer, final int p_size);
}
