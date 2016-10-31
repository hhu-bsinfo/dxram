package de.hhu.bsinfo.utils;

/**
 * Wrapper for accessing GNU readline lib
 *
 * @author Michael Schoettner, michael.schoettner@hhu.de, 07.09.2015
 */
public final class JNIconsole {

    /**
     * Constructor
     */
    private JNIconsole() {
    }

    /**
     * Provide the path to the native implementation.
     *
     * @param p_pathNativeLibrary
     *     Path to the library with the native implementation.
     */
    public static void load(final String p_pathNativeLibrary) {
        System.load(p_pathNativeLibrary);
    }

    /**
     * Provide commands for autocomplete using the tab key and enable it.
     *
     * @param p_commands
     *     Commands to be used for autocomplete
     */
    public static native void autocompleteCommands(String[] p_commands);

    /**
     * Add a string to the history of the console.
     * Use this to load a history from a file and add the commands executed.
     *
     * @param p_str
     *     String to add to the history.
     */
    public static native void addToHistory(String p_str);

    /**
     * Read one line from console.
     *
     * @param p_prompt
     *     The terminal prompt to display.
     * @return the read line
     */
    public static native byte[] readline(String p_prompt);

}
