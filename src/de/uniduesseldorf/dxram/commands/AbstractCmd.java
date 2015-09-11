
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.utils.JNIconsole;

/**
 * Base class for commands
 * @author Michael Schoettner 03.09.2015
 *         Syntax must be described using a string, keywords:
 *         STR string, any chars
 *         PNR positive number, e.g. LID (may be 0)
 *         ANID any existing NID
 *         PNID any existing peer NID
 *         SNID any existing superpeer NID
 *         , for NID,LID tuples
 *         [] optional parameters (at the end only)
 */
public abstract class AbstractCmd {

	/**
	 * Get name of command.
	 * @return the name
	 */
	public abstract String getName();

	/**
	 * Get usage message of command.
	 * @return the usage message
	 */
	public abstract String getUsageMessage();

	/**
	 * Get help message of command.
	 * @return the help message
	 */
	public abstract String getHelpMessage();

	/**
	 * Get syntax string for command.
	 * @return the syntax string
	 */
	public abstract String getSyntax();

	/**
	 * Execution method of a command.
	 * Called from local node, parameters are checked before.
	 * @param p_command
	 *            the string entered by the user
	 * @return true: success, false: failed
	 */
	public abstract boolean execute(String p_command);

	/**
	 * Execution method of a command.
	 * Called on remote node, if overwritten.
	 * Some methods may me implemented in ChunkHandler or LookupHandler
	 * @param p_command
	 *            the string received from sending node
	 * @return result string, depending on command
	 */
	public String remoteExecute(final String p_command) {
		return "error: remote_execute not implemented";
	}

	/**
	 * Print usage message.
	 */
	public void printUsgae() {
		System.out.println("  usage: " + getUsageMessage());
	}

	/**
	 * Print help message.
	 */
	public void printHelpMsg() {
		String[] lines;

		System.out.println("  usage:       " + getUsageMessage());
		System.out.println();

		lines = getHelpMessage().split("\n");
		// we should never end up here
		if (lines == null) {
			return;
		}
		System.out.println("  description: " + lines[0]);
		for (int i = 1; i < lines.length; i++) {
			System.out.println("               " + lines[i]);
		}
	}

	/**
	 * Ask user to proceed or not
	 * @return true: yes, false: no
	 */
	public boolean areYouSure() {
		boolean ret;
		byte[] arr;

		while (true) {
			System.out.print("Are you sure (y/n)?");

			arr = JNIconsole.readline();
			if (arr != null) {
				if (arr[0] == 'y' || arr[0] == 'Y') {
					ret = true;
					break;
				} else if (arr[0] == 'n' || arr[0] == 'N') {
					ret = false;
					break;
				}
			} else {
				ret = false;
				break;
			}
		}

		return ret;
	}

	/**
	 * Check syntax & semantic of token of a command using expected token (from syntax string)
	 * (This method is used by 'areParametersSane')
	 * @param p_expected
	 *            expected token
	 * @param p_found
	 *            actual token of command
	 * @return true: syntax is OK, false: syntax error
	 */
	private boolean parseToken(final String p_expected, final String p_found) {
		boolean ret = true;

		if (p_expected.compareTo("STR") == 0 || p_expected.compareTo("[STR]") == 0) {
			ret = true;
		} else if (p_expected.compareTo("PNR") == 0 || p_expected.compareTo("[PNR]") == 0) {
			try {
				Long.parseLong(p_found);
			} catch (final NumberFormatException nfe) {
				System.out.println("  error: expected positive number but found '" + p_found + "'");
				ret = false;
			}
		} else if (p_expected.compareTo("ANID") == 0 || p_expected.compareTo("[ANID]") == 0 || p_expected.compareTo("PNID") == 0
				|| p_expected.compareTo("[PNID]") == 0 || p_expected.compareTo("SNID") == 0 || p_expected.compareTo("[SNID]") == 0) {

			// do we have a short number?
			try {
				Short.parseShort(p_found);
			} catch (final NumberFormatException nfe) {
				System.out.println("  error: expected NID number but found '" + p_found + "'");
				ret = false;
			}

			if (ret) {
				if (CmdUtils.checkNID(p_found).compareTo("unknown") == 0) {
					System.out.println("  error: unknwon NID '" + p_found + "'");
					ret = false;
				} else {
					if (p_expected.compareTo("PNID") == 0 || p_expected.compareTo("[PNID]") == 0) {
						if (CmdUtils.checkNID(p_found).compareTo("superpeer") == 0) {
							System.out.println("  error: superpeer NID not allowed '" + p_found + "'");
							ret = false;
						}
					} else if (p_expected.compareTo("SNID") == 0 || p_expected.compareTo("[SNID]") == 0) {
						if (CmdUtils.checkNID(p_found).compareTo("peer") == 0) {
							System.out.println("  error: peer NID not allowed '" + p_found + "'");
							ret = false;
						}
					}
				}
			}
		}

		return ret;
	}

	/**
	 * Check if parameters of a given command are sane according to its syntax definition.
	 * (arguments[0] is the command name)
	 * @param p_arguments
	 *            tokens of the given command
	 * @return true: parameters are OK, false: syntax error
	 */
	public boolean areParametersSane(final String[] p_arguments) {
		boolean ret = true;
		final String[] token = getSyntax().split(" ");
		String[] subarg;
		String[] subtoken;

		// too many params?
		// System.out.println("token.length="+token.length+", p_arguments.length="+p_arguments.length);
		if (p_arguments.length > token.length) {
			System.out.println("  error: too many arguments");
			ret = false;
		} else {
			// parse and check params of command
			for (int i = 1; i < token.length; i++) {
				// not enough params?
				if (i >= p_arguments.length) {
					if (token[i].indexOf('[') >= 0) {
						ret = true;
					} else {
						System.out.println("  error: argument missing");
						printUsgae();
						ret = false;
					}
					break;
				}

				// check next expected symbol?
				// System.out.println(i+". token:"+token[i]);

				// is a tuple NID,LID expected next?
				if (token[i].indexOf(',') >= 0) {
					subtoken = token[i].split(",");
					subarg = p_arguments[i].split(",");
					if (subarg == null || subarg.length < 2) {
						System.out.println("  error: expected NID,LID tuple but found '" + p_arguments[i] + "'");
						ret = false;
					} else if (!parseToken(subtoken[0], subarg[0])) {
						ret = false;
					} else if (!parseToken(subtoken[1], subarg[1])) {
						ret = false;
					}
				}

				if (!parseToken(token[i], p_arguments[i])) {
					ret = false;
				}
			}
		}

		return ret;
	}

}
