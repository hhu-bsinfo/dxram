
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Delete a chunk.
 * @author Michael Schoettner 15.09.2015
 */
public class CmdDel extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdDel() {}

	@Override
	public String getName() {
		return "del";
	}

	@Override
	public String getUsageMessage() {
		return "del NodeID,LocalID [-dest=NodeID]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Delete chunk NodeID,LocalID.\n";
		final String line2 = "-dest=NodeID sent request to node NodeID (must not be a superpeer).\n";
		return line1 + line2;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID,PNR"};
		return ret;
	}

	@Override
	public String[] getOptParams() {
		final String[] ret = {"-dest=PNID"};
		return ret;
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		String[] arguments;
		short nodeID;

		try {
			arguments = p_command.split(" ");

			// System.out.println("del: command="+p_command);
			// System.out.println("del: arguments.length="+arguments.length);

			if (arguments.length < 3) {
				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
			} else {
				final String[] v = arguments[2].split("=");
				nodeID = CmdUtils.getNIDfromString(v[1]);
			}

			final String res = Core.executeChunkCommand(nodeID, p_command, true);

			// did we get an error message back?
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

	@Override
	public String remoteExecute(final String p_command) {
		String ret;
		String[] arguments;

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				Core.remove(CmdUtils.getCIDfromTuple(arguments[1]));

				ret = "  Chunk deleted.";
			} catch (final DXRAMException e) {
				System.out.println("Core.remove failed.");
				ret = "  error: 'delete' failed";
			}
		}

		return ret;
	}

}
