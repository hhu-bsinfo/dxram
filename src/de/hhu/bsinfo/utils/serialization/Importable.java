
package de.hhu.bsinfo.utils.serialization;

/**
 * Interface defining an object which can be imported/de-serialized.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.12.15
 */
public interface Importable extends ObjectSize {
	/**
	 * Import/De-serialize this object using the provided importer (target).
	 * Don't call this explicitly. Use an Importer which is calling this
	 * method implicitly.
	 *
	 * @param p_importer Target to import/de-serialize the object from.
	 */
	void importObject(final Importer p_importer);
}
