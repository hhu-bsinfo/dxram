
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Migrate a chunk.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdMigrate extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdMigrate() {}

	@Override
	public String getName() {
		return "migrate";
	}

	@Override
	public String getUsageMessage() {
		return "migrate NID,LID destNID";
	}

	@Override
	public String getHelpMessage() {
		return "Migrate chunk NID,LID to destination node destNID";
	}

	@Override
	public String getSyntax() {
		return "migrate PNID,PNR PNID";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		short nodeID;
		String res;
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			/*
			 * System.out.println("migrate: command="+p_command);
			 * System.out.println("migrate: arguments.length="+arguments.length);
			 */
			nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

			res = Core.executeChunkCommand(nodeID, p_command, true);

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
		short nodeID;
		short destNID;
		long localID;
		long chunkID;
		boolean result;
		String[] arguments;

		// System.out.println("remote_execute: migrate");

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
				localID = CmdUtils.getLIDfromTuple(arguments[1]);
				destNID = CmdUtils.getNIDfromString(arguments[2]);

				System.out.println("migrating chunk " + nodeID + "," + localID + " to " + destNID);

				chunkID = CmdUtils.calcCID(nodeID, localID);

				result = Core.migrate(chunkID, destNID);

				if (!result) {
					ret = "  error: migration failed";
				} else {
					ret = "  Chunk migrated.";
				}
			} catch (final DXRAMException e) {
				System.out.println("   DXRAMException");
				ret = "  error: 'get' failed";
			}
		}

		return ret;
	}

}
