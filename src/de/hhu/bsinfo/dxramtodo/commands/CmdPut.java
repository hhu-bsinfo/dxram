
package de.hhu.bsinfo.dxramtodo.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.dxram.Core;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.engine.DXRAMException;

/**
 * Update an existing chunk
 * @author Michael Schoettner 18.09.2015
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
		return "put NodeID,LocalID text";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Update existing chunk NodeID,LocalID with data 'text'.\n";
		final String line2 = "(Data size of the chunk remains unchanged. Data may be cut)\n\n";
		final String line3 = "Returns success or error";
		return line1 + line2 + line2 + line3;
	}

	@Override
	public String[] getMandParams() {
		final String[] ret = {"PNID,LocalID", "STR"};
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
		String res;
		String[] arguments;

		try {
			arguments = p_command.split(" ");
			nodeID = CmdUtils.getNIDfromTuple(arguments[1]);

			res = Core.executeChunkCommand(nodeID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				ret = false;
			} else {
				System.out.println(res);
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
		int size = -1;

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				// System.out.println("  Update chunk "+arguments[1]+", with data="+arguments[2]);

				// get local chunk from tuple NodeID,LocalID
				c = Core.get(CmdUtils.getCIDfromTuple(arguments[1]));
				if (c == null) {
					// System.out.println("  error: chunk not found, may be it has been migrated");
					ret = "  error: chunk not found, may be it has been migrated";
				} else {

					// get byte buffer of chunk
					final byte[] newData = arguments[2].getBytes();
					size = arguments[2].length();
					if (size > c.getSize()) {
						size = c.getSize();
					}
					final ByteBuffer b = c.getData();
					b.clear();
					for (int i = 0; i < size; i++) {
						b.put(newData[i]);
					}

					Core.put(c);
					ret = "  success: chunk updated.";
				}
			} catch (final DXRAMException e) {
				// System.out.println("  error: unknown error");
				ret = "  error: 'put' failed";
			}
		}

		return ret;
	}

}
