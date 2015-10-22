
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Create a new chunk
 * @author Michael Schoettner 18.09.2015
 */
public class CmdRecover extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdRecover() {}

	@Override
	public String getName() {
		return "recover";
	}

	@Override
	public String getUsageMessage() {
		return "recover failedNodeID restorerNodeID [-f]";
	}

	@Override
	public String getHelpMessage() {
		final String line = "Recover all Chunks from peer with failedNodeID (validity is not checked) on peer with restorerNodeID.\n";
		final String line2 = "-f: Do not use live data, but recover from log files\n";
		return line + line2;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"STR", "PNID"};
		return ret;
	}

	@Override
	public String[] getOptParams() {
		final String[] ret = {"-f"};
		return ret;
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		short nodeID;
		short dest = -1;
		String[] arguments;

		try {
			arguments = p_command.split(" ");
			// First argument is handled like a String because the peer to recover might not be online
			nodeID = Short.parseShort(arguments[1]);
			dest = CmdUtils.getNIDfromString(arguments[2]);

			if (arguments.length == 4) {
				Core.executeRecoveryCommand(nodeID, dest, false);
			} else {
				Core.executeRecoveryCommand(nodeID, dest, true);
			}

			System.out.println("Initialized recovery of " + nodeID + " on " + dest + ".");
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			ret = false;
		}

		return ret;
	}

	@Override
	public String remoteExecute(final String p_command) {
		return "";
	}

}
