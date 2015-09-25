
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Get info about backups
 * @author Michael Schoettner 07.09.2015
 */
public class CmdBackups extends AbstractCmd {
	/**
	 * Constructor
	 */
	public CmdBackups() {}

	@Override
	public String getName() {
		return "backups";
	}

	@Override
	public String getUsageMessage() {
		return "backups NodeID [-mTree]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about all backups for the given NodeID\n";
		final String line2 = "The command can be send to a peer or superpeer/n";
		final String line3 = "-mTree: Optional parameter. Include migrateTree; only available when requesting peer.";
		return line1 + line2 + line3;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"ANID"};
		return ret;
	}

	@Override
	public String[] getOptParams() {
		final String[] ret = {"-mTree"};
		return ret;
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		String res;
		String[] arguments;
		short nodeID;

		try {
			arguments = p_command.split(" ");

			// get NodeID to send command to
			nodeID = CmdUtils.getNIDfromString(arguments[1]);

			if (CmdUtils.checkNID(Short.toString(nodeID)).compareTo("peer") == 0) {
				// System.out.println("	   backups from peer");
				res = Core.executeChunkCommand(nodeID, p_command, true);
			} else {
				// System.out.println("	   backups from superpeer");
				res = Core.executeLookupCommand(nodeID, p_command, true);
			}

			// process result of remote call
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				ret = false;
			}
			System.out.println(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			ret = false;
		}

		return ret;
	}
}
