
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Delete a chunk.
 * @author Michael Schoettner 03.09.2015
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
		return "del NID,LID [destNID]";
	}

	@Override
	public String getHelpMessage() {
		return "Delete chunk NID,LID.\nOptionally, the request can be sent to node destNID (must not be a superpeer).";
	}

	@Override
	public String getSyntax() {
		return "del PNID,PNR [PNID]";
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
				nodeID = CmdUtils.getNIDfromString(arguments[2]);
				// System.out.println("get from:"+nodeID);
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
