
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Migrate a chunk.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdMigrateRange extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdMigrateRange() {}

	@Override
	public String getName() {
		return "migrateRange";
	}

	@Override
	public String getUsageMessage() {
		return "migrateRange NodeID,LocalID LocalID destNodeID";
	}

	@Override
	public String getHelpMessage() {
		return "Migrate chunks from NodeID,[LocalID,LocalID] to destination node destNodeID";
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID,PNR", "PNR", "PNID"};
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
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			/*
			 * System.out.println("migrate: command="+p_command);
			 * System.out.println("migrate: arguments.length="+arguments.length);
			 */
			nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

			Core.executeChunkCommand(nodeID, p_command, false);

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
		long firstLocalID;
		long lastLocalID;
		boolean result;
		String[] arguments;

		// System.out.println("remote_execute: migrate");

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
				firstLocalID = CmdUtils.getLIDfromTuple(arguments[1]);
				lastLocalID = CmdUtils.getLIDfromString(arguments[2]);
				destNID = CmdUtils.getNIDfromString(arguments[3]);

				System.out.println("migrating chunks " + nodeID + ",[" + firstLocalID + "," + lastLocalID + "] to " + destNID);

				result = Core.migrateRange(CmdUtils.calcCID(nodeID, firstLocalID), CmdUtils.calcCID(nodeID, lastLocalID), destNID);

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
