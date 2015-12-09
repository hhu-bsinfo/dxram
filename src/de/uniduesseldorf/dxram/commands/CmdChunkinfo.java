
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

// AppID später optional abfragen

/**
 * Get info about a chunk
 * @author Michael Schoettner 03.09.2015
 */
public class CmdChunkinfo extends AbstractCmd {
	/**
	 * Constructor
	 */
	public CmdChunkinfo() {}

	@Override
	public String getName() {
		return "chunkinfo";
	}

	@Override
	public String getUsageMessage() {
		return "chunkinfo NodeID,LocalID [-dest=NodeID]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about one chunk defined by NodeID,LocalID. Request is send to NodeID.\n";
		final String line2 = "-dest=NodeID:  destination of request (peer or superpeer)";
		return line1 + line2;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID,PNR"};
		return ret;
	}

	@Override
	public String[] getOptParams() {
		final String[] ret = {"-dest=ANID"};
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
			if (arguments.length > 2) {
				final String[] v = arguments[2].split("=");
				nodeID = CmdUtils.getNIDfromString(v[1]);
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
