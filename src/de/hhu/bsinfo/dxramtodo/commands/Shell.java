
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
		AbstractCmd c;

		m_commandMap = new HashMap<String, AbstractCmd>();

		c = new CmdHelp();
		m_commandMap.put(c.getName(), c);
		c = new CmdRecover();
		m_commandMap.put(c.getName(), c);
		c = new CmdClear();
		m_commandMap.put(c.getName(), c);
		c = new CmdNodes();
		m_commandMap.put(c.getName(), c);
		c = new CmdChunkinfo();
		m_commandMap.put(c.getName(), c);
		c = new CmdLogInfo();
		m_commandMap.put(c.getName(), c);
		c = new CmdMigrate();
		m_commandMap.put(c.getName(), c);
		c = new CmdMigrateRange();
		m_commandMap.put(c.getName(), c);
		c = new CmdQuit();
		m_commandMap.put(c.getName(), c);
		c = new CmdPut();
		m_commandMap.put(c.getName(), c);
		c = new CmdGet();
		m_commandMap.put(c.getName(), c);
		c = new CmdDel();
		m_commandMap.put(c.getName(), c);
		c = new CmdCIDT();
		m_commandMap.put(c.getName(), c);
		c = new CmdBackups();
		m_commandMap.put(c.getName(), c);
		c = new CmdStats();
		m_commandMap.put(c.getName(), c);
		c = new CmdCreate();
		m_commandMap.put(c.getName(), c);
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
