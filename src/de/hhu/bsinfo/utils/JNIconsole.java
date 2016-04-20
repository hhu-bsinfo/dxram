
package de.hhu.bsinfo.utils;

/**
 * Wrapper for accessing GNU readline lib
 * @author Michael Schoettner 07.09.2015
 */
public final class JNIconsole {

	/**
	 * Constructor
	 */
	private JNIconsole() {}

	/**
	 * Provide the path to the native implementation.
	 * @param p_pathNativeLibrary
	 *            Path to the library with the native implementation.
	 */
	public static void load(final String p_pathNativeLibrary) {
		System.load(p_pathNativeLibrary);
	}

	/**
	 * Provide commands for autocompletion using the tab key and enable it.
	 * @param p_commands
	 *            Commands to be used for autocompletetion
	 */
	public static native void autocompleteCommands(final String[] p_commands);

	/**
	 * Read one line from console.
	 * @param p_prompt
	 *            The terminal prompt to display.
	 * @return the read line
	 */
	public static native byte[] readline(final String p_prompt);

}
