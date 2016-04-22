
package de.hhu.bsinfo.utils.reflect.unit;

/**
 * Base class for a conversion interface, that handles converting
 * some input value to some output. Typical usage: megabyte to byte
 * on an integer/long value.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public interface UnitConverter {
	/**
	 * Get the string identifier for the unit we are targeting.
	 * @return String identifier of targeting data type.
	 */
	String getUnitIdentifier();

	/**
	 * Convert the specified value.
	 * @param p_value
	 *            Value to convert.
	 * @return Ouput of the conversion or null if failed.
	 */
	Object convert(final Object p_value);
}
