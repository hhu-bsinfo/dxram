
package de.uniduesseldorf.dxram.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;

/**
 * Create a new chunk
 * @author Michael Schoettner 18.09.2015
 */
public class CmdCreate extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdCreate() {}

	@Override
	public String getName() {
		return "create";
	}

	@Override
	public String getUsageMessage() {
		return "create NID text [-size=nbytes] [-name=string]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Create a new chunk with data 'text' on node NID.\n";
		final String line2 = "-size=bytes define size of the chunk in bytes.\n";
		final String line3 = "(The 'text' will be stored at the beginning)\n\n";
		final String line4 = "Returns CID of created chunk in tuple format (NID,LID)";
		return line1 + line2 + line3+line4;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID", "STR"};
	    return ret;
	}

	@Override
    public  String[] getOptParams() {
        final String[] ret = {"-size=PNR", "-name=STR"};
        return ret;
    }

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		short nodeID;
		String res;
		String newCID;
		String[] arguments;

		try {
			arguments = p_command.split(" ");
			nodeID = CmdUtils.getNIDfromString(arguments[1]);

			res = Core.executeChunkCommand(nodeID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				ret = false;
			} else {
				// the call succeed, try to get the CID of the created chunk
				arguments = res.split(" ");
				newCID = CmdUtils.getTupleFromCIDstring(arguments[1]);

				System.out.println("  Created new chunk with CID=(" + newCID + ")");
			}
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			ret = false;
		}

		return ret;
	}

	@Override
	public String remoteExecute(final String p_command) {
		String ret = null;
		Chunk c = null;
		String[] arguments;
		String name=null;
		int size = -1;

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				// get size of data
				size = arguments[2].length();

				// get any optional params
				if (arguments.length>2) {
					for (int i=3; i<arguments.length; i++) {
						if (arguments[i].indexOf("-size")>=0) {
							final String[] sizeArg = arguments[i].split("=");
							size = Integer.parseInt(sizeArg[1]);
						} else if (arguments[i].indexOf("-name")>=0) {
							final String[] nameArg = arguments[i].split("=");
							name = nameArg[1];
						}
					}
				}
				System.out.println("  Create chunk with size="+size+", name="+name);
				if (name!=null) {
					c = Core.createNewChunk(size, name);
				} else {
					c = Core.createNewChunk(size);
				}
				if (c == null) {
					ret = "  error: createNewChunk failed";
				} else {
					final ByteBuffer b = c.getData();
					b.put(arguments[2].getBytes());

					Core.put(c);
					ret = "success: " + Long.toString(c.getChunkID());
				}
			} catch (final DXRAMException e) {
				System.out.println("  error: Core.createNewChunk failed");
				ret = "  error: 'put' failed";
			}
		}

		return ret;
	}

}
