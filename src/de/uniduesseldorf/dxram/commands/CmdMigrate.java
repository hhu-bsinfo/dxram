
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public class CmdMigrate extends Cmd {

	@Override
	public String get_name() {
		return "migrate";
	}

	@Override
	public String get_usage_message() {
		return "migrate NID,LID destNID";
	}

	@Override
	public String get_help_message() {
		return "Migrate chunk NID,LID to destination node destNID";
	}

	@Override
	public String get_syntax() {
		return "migrate PNID,PNR PNID";
	}

	// called after parameter have been checked
	@Override
	public int execute(final String p_command) {
		String[] arguments;

		try {
			arguments = p_command.split(" ");

			/*
			 * System.out.println("migrate: command="+p_command);
			 * System.out.println("migrate: arguments.length="+arguments.length);
			 */
			short NID = CmdUtils.get_NID_from_tuple(arguments[1]);

			String res = Core.execute_chunk_command(NID, p_command, true);

			System.out.println(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
		}
		return 0;
	}

	@Override
	public String remote_execute(final String p_command) {
		String[] arguments;
		boolean ret;

		// System.out.println("remote_execute: migrate");

		if (p_command == null) {
			return "  error: internal error";
		}

		try {
			arguments = p_command.split(" ");

			short NID = CmdUtils.get_NID_from_tuple(arguments[1]);
			long LID = CmdUtils.get_LID_from_tuple(arguments[1]);
			short destNID = CmdUtils.get_NID_from_string(arguments[2]);

			System.out.println("migrating chunk " + NID + "," + LID + " to " + destNID);

			long CID = CmdUtils.calc_CID(NID, LID);

			ret = Core.migrate(CID, destNID);

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
