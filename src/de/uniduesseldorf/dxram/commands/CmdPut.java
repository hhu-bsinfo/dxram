
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
	public CmdPut() {}

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
		return line1 + line2 + line3;
	}

	@Override
	public String getSyntax() {
		return "put PNID STR [STR]";
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

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				// copy data from command to ByteBuffer of chunk
				arguments = p_command.split(" ");

				if (arguments[2].toLowerCase().startsWith("size:")) {
					// create chunk with name?
					if (arguments.length > 3) {
						c = Core.createNewChunk(Integer.parseInt(arguments[2].split(":")[1]), arguments[3]);
						if (c == null) {
							ret = "  error: createNewChunk failed";
						}
					} else {
						c = Core.createNewChunk(Integer.parseInt(arguments[2].split(":")[1]));
						if (c == null) {
							ret = "  error: createNewChunk failed";
						}
					}

					if (ret == null) {
						// now save the chunk
						Core.put(c);

						ret = "success: " + Long.toString(c.getChunkID());
					}
				} else {
					// create chunk with name?
					if (arguments.length > 3) {
						c = Core.createNewChunk(arguments[2].length(), arguments[3]);
						if (c == null) {
							ret = "  error: createNewChunk failed";
						}
					} else {
						c = Core.createNewChunk(arguments[2].length());
						if (c == null) {
							ret = "  error: createNewChunk failed";
						}
					}

					if (ret == null) {
						final ByteBuffer b = c.getData();
						b.put(arguments[2].getBytes());

						// now save the chunk
						Core.put(c);
						ret = "success: " + Long.toString(c.getChunkID());
					}
				}
			} catch (final DXRAMException e) {
				System.out.println("  error: Core.createNewChunk failed");
				ret = "  error: 'put' failed";
			}
		}

		return ret;
	}

}
