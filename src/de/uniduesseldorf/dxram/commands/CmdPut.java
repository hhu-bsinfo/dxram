
package de.uniduesseldorf.dxram.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Save a new chunk
 * @author Michael Schoettner 03.09.2015
 */
public class CmdPut extends AbstractCmd {

	/**
	 * Constructor
	 */
	public CmdPut() {
	}

	@Override
	public String getName() {
		return "put";
	}

	@Override
	public String getUsageMessage() {
		return "put NID text [strID] ";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Save data 'text' on node NID.\n";
		final String line2 = "Optionally, you can provide a name 'strID' to retrieve a chunk by name.\n";
		final String line3 = "Returns CID of created chunk in tuple format (NID,LID)";
		return line1+line2+line3;
	}

	@Override
	public String getSyntax() {
		return "put PNID STR [STR]";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		String[] arguments;

		try {
			arguments = p_command.split(" ");
			final short nodeID = CmdUtils.getNIDfromString(arguments[1]);

			final String res = Core.executeChunkCommand(nodeID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				return false;
			}

			// the call succeed, try to get the CID of the created chunk
			arguments = res.split(" ");
			final String newCID = CmdUtils.getTupleFromCIDstring(arguments[1]);

			System.out.println("  Created new chunk with CID=(" + newCID + ")");

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
			return false;
		}
		return true;
	}

	@Override
	public String remoteExecute(final String p_command) {
		Chunk c = null;
		String[] arguments;

		if (p_command == null) {
			return "  error: internal error";
		}
		try {
			// copy data from command to ByteBuffer of chunk
			arguments = p_command.split(" ");

			// create chunk with name?
			if (arguments.length > 3) {
				c = Core.createNewChunk(p_command.length(), arguments[3]);
				if (c == null) {
					return "  error: createNewChunk failed";
				}
			} else {
				c = Core.createNewChunk(p_command.length());
				if (c == null) {
					return "  error: createNewChunk failed";
				}
			}

			final ByteBuffer b = c.getData();
			b.put(arguments[2].getBytes());

			// now save the chunk
			Core.put(c);
			return "success: " + Long.toString(c.getChunkID());
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.createNewChunk failed");
		}
		return "  error: 'put' failed";
	}

}
