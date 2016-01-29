
package de.hhu.bsinfo.dxramtodo.commands;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.hhu.bsinfo.utils.JNIconsole;

/**
 * Interactive shell
 * @author Michael Schoettner 17.08.2015
 */
public final class Shell {

	// Attributes
	private static Map<String, AbstractCmd> m_commandMap;

	// create & store all commands in HashMap
	static {

	}

	/**
	 * Constructor
	 */
	private Shell() {}

	/**
	 * Get command object.
	 * @param p_cmd
	 *            the command String
	 * @return reference to command object
	 */
	public static AbstractCmd getCommand(final String p_cmd) {
		return m_commandMap.get(p_cmd);
	}

	/**
	 * Get command object.
	 * @return reference to all commands (String set)
	 */
	public static Set<String> getAllCommands() {
		return m_commandMap.keySet();
	}

	/**
	 * Command loop.
	 */
	public static void loop() {
		String command;
		String[] arguments;
		byte[] arr;

		System.out.println("DXRAM shell v. 0.1");

		while (true) {

			arr = JNIconsole.readline();
			if (arr != null) {
				command = new String(arr, 0, arr.length);
				// System.out.println("Java: *"+command+"*");
				arguments = command.split(" ");

				final AbstractCmd c = m_commandMap.get(arguments[0]);
				if (c == null) {
					System.out.println("error: unknown command");
				} else {
					if (c.areParametersSane(arguments)) {
						c.execute(command);
					} else {
						c.printUsage();
					}
				}
			}
		}
	}

}
