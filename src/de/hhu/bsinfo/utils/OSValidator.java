package de.hhu.bsinfo.utils;

/**
 * Check which operating system the application is running on.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 04.04.16
 *
 */
public class OSValidator {

	private static String OS = System.getProperty("os.name").toLowerCase();

	public static final String MS_WINDOWS = "win";
	public static final String MS_OSX = "osx";
	public static final String MS_UNIX = "uni";
	public static final String MS_SOLARIS = "sol";
	public static final String MS_UNKNOWN_OS = "unkn";
	
	/**
	 * Check if running on windows.
	 * @return True if windows, false otherwise.
	 */
	public static boolean isWindows() {
		return (OS.indexOf("win") >= 0);
	}

	/**
	 * Check if running on mac osx.
	 * @return True if mac osx, false otherwise. 
	 */
	public static boolean isMac() {
		return (OS.indexOf("mac") >= 0);
	}

	/**
	 * Check if running on unix like systems.
	 * @return True if unix like system, false otherwise.
	 */
	public static boolean isUnix() {
		return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0);
	}

	/**
	 * Check if running on solaris.
	 * @return True if solaris, false otherwise.
	 */
	public static boolean isSolaris() {
		return (OS.indexOf("sunos") >= 0);
	}
	
	/**
	 * Get the string of the system running on.
	 * @return String of system running on (refer to final statics of this class).
	 */
	public static String getOS() {
		if (isWindows()) {
			return "win";
		} else if (isMac()) {
			return "osx";
		} else if (isUnix()) {
			return "uni";
		} else if (isSolaris()) {
			return "sol";
		} else {
			return "unkn";
		}
	}

}