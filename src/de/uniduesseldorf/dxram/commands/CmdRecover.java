
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
		return "recover failedNodeID restorerNodeID";
	}

	@Override
	public String getHelpMessage() {
		final String line = "Recover all Chunks from peer with first NID on peer with second NID.\n";
		return line;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID", "PNID"};
		return ret;
	}

	@Override
	public String[] getOptParams() {
		return null;
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
			nodeID = CmdUtils.getNIDfromString(arguments[1]);
			dest = CmdUtils.getNIDfromString(arguments[2]);

			Core.executeRecoveryCommand(nodeID, dest);

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
