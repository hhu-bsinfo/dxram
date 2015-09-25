package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Get info about backups
 * @author Michael Schoettner 24.09.2015
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
		return "backups NID [-dest=SNID]";
//		return "backups NID [-dest=SNID] [-mTree]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about all backups for the given NID\n";
		final String line2 = "The command can be send to a peer or superpeer/n";
		final String line3 = "-dest: send request to superpeer SNID/n";
		//final String line4 = "-mTree: Optional parameter. Include migrateTree; only available when requesting peer.";
		return line1+line2+line3;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"ANID"};
	    return ret;
	}

	@Override
    public  String[] getOptParams() {
        final String[] ret = {"-dest=SNID"};
        return ret;
    }

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		String res;
		String[] arguments;
		short nodeID;
		boolean ret=true;

		try {
			arguments = p_command.split(" ");

			if (arguments.length < 3) {
				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
				res = Core.executeChunkCommand(nodeID, p_command, true);
			} else {
				final String[] v = arguments[2].split("=");
				nodeID = CmdUtils.getNIDfromString(v[1]);
				res = Core.executeLookupCommand(nodeID, p_command, true);
			}

			// process result of remote call
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				ret=false;
			}
			System.out.print(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			ret=false;
		}

		return ret;
	}
}
