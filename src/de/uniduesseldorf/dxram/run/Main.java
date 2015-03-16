
package de.uniduesseldorf.dxram.run;

import java.lang.reflect.Method;

/**
 * Main-Class for the jar-File
 * @author Florian Klein
 *         14.06.2012
 */
public final class Main {

	// Constructors
	/**
	 * Creates an instance of Main
	 */
	private Main() {}

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
			for (int i = 0;i < arguments.length;i++) {
				arguments[i] = p_arguments[i + 1];
			}

			try {
				c = Class.forName("de.uniduesseldorf.dxram.test." + p_arguments[0] + "Test");
				m = c.getMethod("main", String[].class);
				m.invoke(null, new Object[] {arguments});
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}
	}

}
