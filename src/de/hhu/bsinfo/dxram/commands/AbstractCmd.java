
package de.hhu.bsinfo.dxram.commands;

import java.io.UnsupportedEncodingException;

import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.Tools;

/**
 * Base class for commands
 * @author Michael Schoettner 16.09.2015
 *         Syntax must be described using a string, keywords:
 *         STR string, any chars
 *         PNR positive number, e.g. LocalID (may be 0)
 *         ANID any existing NodeID
 *         PNID any existing peer NodeID
 *         SNID any existing superpeer NodeID
 *         , for NodeID,LocalID tuples
 *         [] optional parameter; only one and at the end only.
 *         [-STR]
 *         or [-STR=STR|PNR|ANID|PNID|SNID]
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
	 * Get syntax & semantic of mandatory parameters (order will be enforced)
	 * @return the syntax string
	 */
	public abstract String[] getMandParams();

	/**
	 * Get syntax & semantic of optional parameters (any order)
	 * @return the syntax string
	 */
	public abstract String[] getOptParams();

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
	public void printUsage() {
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
	 * Check syntax and semantic of a single given parameter
	 * @param p_expectedParam
	 *            expected parameter
	 * @param p_givenParam
	 *            given parameter
	 * @return true: no problems, false: errors
	 */
	private static boolean checkSingleParam(final String p_expectedParam, final String p_givenParam) {
		boolean ret = true;
		String pattern = "";

		// System.out.println("p_expectedParam="+p_expectedParam+", p_givenParam="+p_givenParam);

		// next is a NodeID,LocalID tuple?
		if (p_expectedParam.indexOf(",") > 1) {
			// check syntax
			pattern = pattern + "-?([0-9])+,([0-9])+";
			ret = p_givenParam.matches(pattern);
			if (!ret) {
				System.out.println("  error: bad parameter: '" + p_givenParam + "'");
				// check semantic
			} else {
				final String[] chunkIDtuple = p_givenParam.split(",");
				ret = checkValidNode(p_expectedParam, chunkIDtuple[0]);
			}

			// next is a node number (positive or negative)?
		} else if (p_expectedParam.compareTo("PNID") == 0
				|| p_expectedParam.compareTo("SNID") == 0
				|| p_expectedParam.compareTo("ANID") == 0) {
			pattern = pattern + "-?([0-9])+";
			ret = p_givenParam.matches(pattern);
			if (!ret) {
				System.out.println("  error: bad parameter. Given '" + p_givenParam + "' but expected was '" + p_expectedParam + "'");
				// check semantic
			} else {
				ret = checkValidNode(p_expectedParam, p_givenParam);
			}

			// next is a positve number >0 ?
		} else if (p_expectedParam.compareTo("PNR") == 0) {
			pattern = pattern + "([0-9])+";
			ret = p_givenParam.matches(pattern);

			// next is a string?
		} else if (p_expectedParam.compareTo("STR") == 0) {
			try {
				ret = Tools.looksLikeUTF8(p_givenParam.getBytes());
			} catch (final UnsupportedEncodingException e) {
				System.out.println("  error: bad parameter. Expected was a string but parameter has unsupported encoding.");
				ret = false;
			}
			if (!ret) {
				System.out.println("  error: bad parameter. Given '" + p_givenParam + "' but expected was a string.");
			}
		} else {
			System.out.println("  error: bad token '" + p_expectedParam + "' in syntax definition");
			ret = false;
		}

		return ret;
	}

	/**
	 * Check if there are duplicates in the given params
	 * @param p_search
	 *            argument to check for duplication
	 * @param p_givenParams
	 *            given params
	 * @param p_foundIdx
	 *            position of p_search in p_givenParams
	 * @return true: p_search is duplicated, false: OK
	 */
	private static boolean duplicatesInOptParams(final String p_search, final String[] p_givenParams, final int p_foundIdx) {
		boolean duplicates = false;

		for (int i = 1; i < p_givenParams.length; i++) {
			final String[] kvParam2 = p_givenParams[i].split("=");
			if (p_search.compareTo(kvParam2[0]) == 0 && i != p_foundIdx) {
				duplicates = true;
				System.out.println("  error: duplicate parameter '" + p_search + "'");
				break;
			}
		}
		return duplicates;
	}

	/**
	 * Get the given arguments (arguments[0] is the command name)
	 * @param p_arguments
	 *            tokens of the given command
	 * @return given parameters (may be null if none are given)
	 */
	private String[] getGivenArguments(final String[] p_arguments) {
		String[] givenParams = null;

		// get the given params
		if (p_arguments.length > 1) {
			givenParams = new String[p_arguments.length - 1];
			// System.out.print("   givenParams: ");
			for (int i = 1; i < p_arguments.length; i++) {
				givenParams[i - 1] = p_arguments[i];
				// System.out.print(givenParams[i-1]+"; ");
			}
			// System.out.println();
		}
		return givenParams;
	}

	/**
	 * Check if parameters of a given command are sane according to its syntax and semantic definition.
	 * (arguments[0] is the command name)
	 * @param p_arguments
	 *            tokens of the given command
	 * @return true: parameters are OK, false: syntax error
	 */
	public boolean areParametersSane(final String[] p_arguments) {
		final String[] expectedMandParams = getMandParams();
		final String[] expectedOptParams = getOptParams();
		String[] givenParams = null;
		// String pattern=null;
		boolean ret = true;
		int nrOfexpectedMandParams;
		int nrOfexpectedOptParams;

		givenParams = getGivenArguments(p_arguments);

		// calc number of expected params
		if (expectedMandParams == null) {
			nrOfexpectedMandParams = 0;
		} else {
			nrOfexpectedMandParams = expectedMandParams.length;
		}
		// calc number of max. optional params
		if (expectedOptParams == null) {
			nrOfexpectedOptParams = 0;
		} else {
			nrOfexpectedOptParams = expectedOptParams.length;
		}

		// no params expected and none given?
		if (givenParams == null && nrOfexpectedMandParams == 0) {
			ret = true;
			// not enough mandatory params?
		} else if ((givenParams == null && nrOfexpectedMandParams > 0) || (givenParams.length < nrOfexpectedMandParams)) {
			System.out.println("  error: not enough mandatory parameters");
			ret = false;
			// too much params?
		} else if (givenParams.length > (nrOfexpectedMandParams + nrOfexpectedOptParams)) {
			System.out.println("  error: too many parameters");
			ret = false;
		} else {
			// check syntax of mandatory parameters
			if (expectedMandParams != null) {
				for (int i = 0; i < expectedMandParams.length; i++) {
					ret = checkSingleParam(expectedMandParams[i], givenParams[i]);
					// ret = givenParams[i].matches(pattern);
					if (!ret) {
						// System.out.println("  error: bad parameter: '"+givenParams[i]+"'");
						break;
					}
				}
			}

			// check syntax of any optional parameters, if allowed and no errors so far
			if (expectedOptParams != null && ret && givenParams.length > nrOfexpectedMandParams) {

				// iterate over given optional params
				boolean found;
				// System.out.println("Check optional parameters");
				for (int i = nrOfexpectedMandParams; i < givenParams.length; i++) {
					// System.out.println("   check given param: "+givenParams[i]);
					final String[] givenKVOptParam = givenParams[i].split("=");
					if (givenKVOptParam[0].indexOf("-") < 0) {
						System.out.println("  error: bad optional parameter '" + givenParams[i] + "'");
						ret = false;
						break;
					} else {

						if (!duplicatesInOptParams(givenKVOptParam[0], givenParams, i)) {

							// find param
							found = false;
							for (int j = 0; j < expectedOptParams.length; j++) {
								final String[] expectedKVOptParam = expectedOptParams[j].split("=");

								if (expectedKVOptParam[0].compareTo(givenKVOptParam[0]) == 0) {

									/*
									 * System.out.print("   found expectedKVOptParam[0]="+expectedKVOptParam[0]);
									 * if (expectedKVOptParam.length>1) {
									 * System.out.print(", expectedKVOptParam[1]="+expectedKVOptParam[1]);
									 * }
									 * System.out.println();
									 */
									found = true;

									// -key=value?
									if (expectedKVOptParam.length == 2 && givenKVOptParam.length == 2) {
										ret = checkSingleParam(expectedKVOptParam[1], givenKVOptParam[1]);
										// ret = givenKVOptParam[1].matches(pattern);
										if (!ret) {
											System.out.println("  error: bad parameter: '" + givenParams[i] + "'");
											break;
										}
										// -key?
									} else if (expectedKVOptParam.length == 1 && givenKVOptParam.length == 1) {
										System.out.println("  simple switch");
										ret = true;
										// error?
									} else {
										System.out.println("  error: bad parameter: '" + givenParams[i] + "'");
										ret = false;
									}
								}
							}

							// stop if unknown optional parameter
							if (!found) {
								System.out.println("  error: unknown parameter'" + givenParams[i] + "'");
								ret = false;
								break;
								// stop if known optional parameter but syntax error
							} else if (found && !ret) {
								break;
							}
						} else {
							break;
						}
					}
				}
			}
		}

		return ret;
	}

	/**
	 * Check semantic of NodeID
	 * @param p_expectedNID
	 *            the expected NodeID
	 * @param p_foundNID
	 *            the given NodeID
	 * @return true: parameters are OK, false: syntax error
	 */
	private static boolean checkValidNode(final String p_expectedNID, final String p_foundNID) {
		boolean ret = true;

		if (CmdUtils.checkNID(p_foundNID).compareTo("unknown") == 0) {
			System.out.println("  error: unknown NodeID '" + p_foundNID + "'");
			ret = false;
		} else {
			if (p_expectedNID.compareTo("PNID") == 0) {
				if (CmdUtils.checkNID(p_foundNID).compareTo("superpeer") == 0) {
					System.out.println("  error: superpeer NodeID not allowed '" + p_foundNID + "'");
					ret = false;
				}
			} else if (p_expectedNID.compareTo("SNID") == 0) {
				if (CmdUtils.checkNID(p_foundNID).compareTo("peer") == 0) {
					System.out.println("  error: peer NodeID not allowed '" + p_foundNID + "'");
					ret = false;
				}
			}
		}
		return ret;
	}

}
