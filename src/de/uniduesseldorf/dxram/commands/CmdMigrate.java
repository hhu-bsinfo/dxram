
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
	public CmdMigrate() {
	}

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
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			/*
			 * System.out.println("migrate: command="+p_command);
			 * System.out.println("migrate: arguments.length="+arguments.length);
			 */
			final short nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

			final String res = Core.executeChunkCommand(nodeID, p_command, true);

			System.out.println(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			return false;
		}
		return true;
	}

	@Override
	public String remoteExecute(final String p_command) {
		String[] arguments;
		boolean ret;

		// System.out.println("remote_execute: migrate");

		if (p_command == null) {
			return "  error: internal error";
		}

		try {
			arguments = p_command.split(" ");

			final short nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
			final long localID = CmdUtils.getLIDfromTuple(arguments[1]);
			final short destNID = CmdUtils.getNIDfromString(arguments[2]);

			System.out.println("migrating chunk " + nodeID + "," + localID + " to " + destNID);

			final long chunkID = CmdUtils.calcCID(nodeID, localID);

			ret = Core.migrate(chunkID, destNID);

			if (!ret) {
				return "  error: migration failed";
			} else {
				return "  Chunk migrated.";
			}
		} catch (final DXRAMException e) {
			System.out.println("   DXRAMException");

		}
		return "  error: 'get' failed";
	}

}
