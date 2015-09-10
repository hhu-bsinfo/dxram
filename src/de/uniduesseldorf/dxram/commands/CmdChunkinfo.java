
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

// AppID später optional abfragen

/**
 * Get info about a chunk
 * @author Michael Schoettner 03.09.2015
 */
public class CmdChunkinfo extends AbstractCmd {
	/**
	 * Constructor
	 */
	public CmdChunkinfo() {
	}

	@Override
	public String getName() {
		return "chunkinfo";
	}

	@Override
	public String getUsageMessage() {
		return "chunkinfo NID,LID [destNID]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about chunk NID,LID. Request send to NID.\n";
		final String line2 = "destNID: explicit destination of request (peer or superpeer)";
		return line1+line2;
	}

	@Override
	public String getSyntax() {
		return "chunkinfo PNID,PNR [ANID]";
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
			if (arguments.length > 2) {
				nodeID = CmdUtils.getNIDfromString(arguments[2]);
			} else {
				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
			}

			if (CmdUtils.checkNID(Short.toString(nodeID)).compareTo("peer") == 0) {
				// System.out.println("	   chunkinfo from peer");
				res = Core.executeChunkCommand(nodeID, p_command, true);
			} else {
				// System.out.println("	   chunkinfo from superpeer");
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
