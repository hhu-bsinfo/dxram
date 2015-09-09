
package de.uniduesseldorf.dxram.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Get a chunk.
 * @author Michael Schoettner 03.09.2015
 */
public class CmdGet extends AbstractCmd {
	private static final int MAX_DATA_TRANSFER = 100;

	/**
	 * Constructor
	 */
	public CmdGet() {}

	@Override
	public String getName() {
		return "get";
	}

	@Override
	public String getUsageMessage() {
		return "get NID,LID [destNID]";
	}

	@Override
	public String getHelpMessage() {
		final String line1 = "Get data of chunk NID,LID.\n";
		final String line2 = "Optionally, the request can be sent to node destNID (must not be a superpeer).\n";
		final String line3 = "Important: Only a maximum of 100 byte of data is transfered.";
		return line1 + line2 + line3;
	}

	@Override
	public String getSyntax() {
		return "get PNID,PNR [ANID]";
	}

	// called after parameter have been checked
	@Override
	public boolean execute(final String p_command) {
		boolean ret = true;
		String[] arguments;
		short nodeID;

		try {
			arguments = p_command.split(" ");

			// System.out.println("get: command="+p_command);
			// System.out.println("get: arguments.length="+arguments.length);

			if (arguments.length < 3) {
				nodeID = CmdUtils.getNIDfromTuple(arguments[1]);
			} else {
				nodeID = CmdUtils.getNIDfromString(arguments[2]);
			}

			// nodeID = Short.parseShort(arguments[2]);

			// System.out.println("get from:"+nodeID);

			final String res = Core.executeChunkCommand(nodeID, p_command, true);

			// did we get an error message back?
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

	@Override
	public String remoteExecute(final String p_command) {
		String ret;
		String data = null;
		String[] arguments;
		Chunk c = null;
		ByteBuffer b;

		if (p_command == null) {
			ret = "  error: internal error";
		} else {
			try {
				arguments = p_command.split(" ");

				c = Core.get(CmdUtils.getCIDfromTuple(arguments[1]));
				if (c == null) {
					ret = "  error: CID(" + arguments[1] + ") not found";
				} else {
					b = c.getData();

					// send back max. MAX_DATA_TRANSFER byte
					if (c.getSize() <= MAX_DATA_TRANSFER) {
						data = new String(b.array());
					} else {
						final byte[] buff = new byte[MAX_DATA_TRANSFER];
						for (int i = 0; i < MAX_DATA_TRANSFER - 3; i++) {
							buff[i] = b.get(i);
						}
						for (int i = MAX_DATA_TRANSFER - 3; i < MAX_DATA_TRANSFER; i++) {
							buff[i] = (byte) '.';
						}
						data = new String(buff);
					}
					// System.out.println(data);

					ret = "  Chunk data: " + data;
				}
			} catch (final DXRAMException e) {
				ret = "  error: 'get' failed";
			}
		}

		return ret;
	}

}
