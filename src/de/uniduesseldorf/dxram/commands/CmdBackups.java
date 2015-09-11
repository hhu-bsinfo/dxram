package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Get info about a CIDTable
 * @author Michael Schoettner 07.09.2015
 */
public class CmdBackups extends AbstractCmd {
	/**
	 * Constructor
	 */
	public CmdBackups() {
	}

	@Override
	public String getName() {
		return "backups";
	}

	@Override
	public String getUsageMessage() {
		return "backups NID";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about all backups for the given NID\nThe command can be send to a peer or superpeer";
		return line1;
	}

	@Override
	public String getSyntax() {
		return "backups ANID";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		String res;
		String[] arguments;
		short nodeID;

		try {
			arguments = p_command.split(" ");

			// get NID to send command to
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
				return false;
			}
			System.out.println(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			return false;
		}

		return true;
	}
}
