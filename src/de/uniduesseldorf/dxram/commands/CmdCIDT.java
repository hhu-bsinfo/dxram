
package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Get info about a CIDTable
 * @author Michael Schoettner 07.09.2015
 */
public class CmdCIDT extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdCIDT() {}

	@Override
	public String getName() {
		return "cidt";
	}

	@Override
	public String getUsageMessage() {
		return "cidt NID";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get information about CIDTable of peer NID\n";
		return line1;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID"};
	    return ret;
	}

	@Override
    public  String[] getOptParams() {
        return null;
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

			// get NID to send command to
			nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

			res = Core.executeChunkCommand(nodeID, p_command, true);

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
