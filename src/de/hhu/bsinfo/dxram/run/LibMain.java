package de.hhu.bsinfo.dxram.run;

import java.lang.reflect.Method;

/**
 * Main-Class for the jar-File
 * @author Florian Klein
 *         14.06.2012
 */
public final class LibMain {

	private static final String MS_CLASS_BASE_PATH = "de.hhu.bsinfo.dxram.";
	
	// Constructors
	/**
	 * Creates an instance of Main
	 */
	private LibMain() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		Class<?> c;
		Method m;
		String[] arguments;

		if (p_arguments.length > 0) {
			arguments = new String[p_arguments.length - 1];
			for (int i = 0; i < arguments.length; i++) {
				arguments[i] = p_arguments[i + 1];
			}

			try {
				c = Class.forName("de.hhu.bsinfo.dxram." + p_arguments[0]);
				m = c.getMethod("main", String[].class);
				m.invoke(null, new Object[] {arguments});
			} catch (final Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Specify a runnable class from " + MS_CLASS_BASE_PATH + " as argument.");
		}
	}

}
