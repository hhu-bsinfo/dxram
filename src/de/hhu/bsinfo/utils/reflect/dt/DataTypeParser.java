package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Base class for a parser which processes a string
 * based on a specific data format.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public interface DataTypeParser {
    /**
     * Get the string identifier for the type we are targeting.
     *
     * @return String identifier of targeting data type.
     */
    String getTypeIdentifer();

    /**
     * Get the class identifier for the type we are targeting.
     *
     * @return Class identifier for the targeting data type.
     */
    Class<?> getClassToConvertTo();

    /**
     * Parse the string and create an object instance of it.
     *
     * @param p_str
     *         String to parse.
     * @return Object to be created or null if parsing failed.
     */
    Object parse(String p_str);
}
