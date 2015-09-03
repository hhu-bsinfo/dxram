
package de.uniduesseldorf.dxram.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public class CmdGet extends Cmd {
	private final static int MAX_DATA_TRANSFER = 100;

	@Override
	public String get_name() {
		return "get";
	}

	@Override
	public String get_usage_message() {
		return "get NID,LID [destNID]";
	}

	@Override
	public String get_help_message() {
		return "Get data of chunk NID,LID.\nOptionally, the request can be sent to node destNID (must not be a superpeer).\nImportant: Only a maximum of 100 byte of data is transfered.";
	}

	@Override
	public String get_syntax() {
		return "get PNID,PNR [ANID]";
	}

	// called after parameter have been checked
	@Override
	public int execute(final String p_command) {
		String[] arguments;
		short NID;

		try {
			arguments = p_command.split(" ");

			// System.out.println("get: command="+p_command);
			// System.out.println("get: arguments.length="+arguments.length);

			if (arguments.length < 3) {
				NID = CmdUtils.get_NID_from_tuple(arguments[1]);
			} else {
				NID = CmdUtils.get_NID_from_string(arguments[2]);
			}

			// NID = Short.parseShort(arguments[2]);

			// System.out.println("get from:"+NID);

			String res = Core.execute_chunk_command(NID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error") > -1) {
				System.out.println(res);
				return -1;
			}

			System.out.println(res);

		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
		}
		return 0;
	}

	@Override
	public String remote_execute(final String p_command) {
		Chunk c = null;
		String data = null;
		String[] arguments;

		if (p_command == null) {
			return "  error: internal error";
		}

		try {
			arguments = p_command.split(" ");

			c = Core.get(CmdUtils.get_CID_from_tuple(arguments[1]));
			if (c == null) {
				return "  error: CID(" + arguments[1] + ") not found";
			}

			ByteBuffer b = c.getData();

			// send back max. MAX_DATA_TRANSFER byte
			if (c.getSize() <= MAX_DATA_TRANSFER) {
				data = new String(b.array());
			} else {
				byte buff[] = new byte[MAX_DATA_TRANSFER];
				for (int i = 0; i < (MAX_DATA_TRANSFER - 3); i++) {
					buff[i] = b.get(i);
				}
				for (int i = MAX_DATA_TRANSFER - 3; i < MAX_DATA_TRANSFER; i++) {
					buff[i] = (byte) '.';
				}
				data = new String(buff);
			}
			// System.out.println(data);

			return "  Chunk data: " + data;

		} catch (final DXRAMException e) {}

		return "  error: 'get' failed";
	}

}
