package de.hhu.bsinfo.utils.args;

/**
 * Interface for a parser to process the string array provided by the java main function
 * to create a MainArguments list of.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface ArgumentListParser {
    /**
     * Parse the string array provided by the java main function.
     *
     * @param p_args
     *         Array of strings from the java main function.
     * @param p_arguments
     *         Argument list to add the parsed arguments to.
     */
    void parseArguments(String[] p_args, ArgumentList p_arguments);
}
