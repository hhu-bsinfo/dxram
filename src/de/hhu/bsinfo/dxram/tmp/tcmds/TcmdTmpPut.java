package de.hhu.bsinfo.dxram.tmp.tcmds;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserByte;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserInt;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserLong;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserShort;

/**
 * Put data into temporary storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TcmdTmpPut extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_ID =
			new ArgumentList.Argument("id", null, false, "Id of the chunk stored in temporary storage");
	private static final ArgumentList.Argument MS_ARG_DATA_TYPE =
			new ArgumentList.Argument("type", "str", true,
					"Type of the data to store (str, byte, short, int, long, hex)");
	private static final ArgumentList.Argument
			MS_ARG_DATA = new ArgumentList.Argument("data", null, false, "Data to store");
	private static final ArgumentList.Argument MS_ARG_OFFSET =
			new ArgumentList.Argument("offset", "-1", true,
					"Offset within the existing to store the new data to. -1 to override existing data");

	@Override
	public String getName() {
		return "tmpput";
	}

	@Override
	public String getDescription() {

		return "Put data in the specified chunk (temporary storage)."
				+ "If no offset is specified, the whole chunk is overwritten with the new data. "
				+ "Otherwise the data is inserted at the starting offset with its length. "
				+ "If the specified data is too long it will be trunced";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_ID);
		p_arguments.setArgument(MS_ARG_DATA_TYPE);
		p_arguments.setArgument(MS_ARG_DATA);
		p_arguments.setArgument(MS_ARG_OFFSET);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Integer id = p_arguments.getArgumentValue(MS_ARG_ID, Integer.class);
		String dataType = p_arguments.getArgumentValue(MS_ARG_DATA_TYPE, String.class);
		String data = p_arguments.getArgumentValue(MS_ARG_DATA, String.class);
		Integer offset = p_arguments.getArgumentValue(MS_ARG_OFFSET, Integer.class);

		TemporaryStorageService tmpStorageService =
				getTerminalDelegate().getDXRAMService(TemporaryStorageService.class);

		Chunk chunk = tmpStorageService.get(id);
		if (chunk == null) {
			getTerminalDelegate()
					.println("Getting chunk " + ChunkID.toHexString(id) + " from temporary storage failed.",
							TerminalColor.RED);
			return true;
		}

		if (offset == -1) {
			// create new chunk
			chunk = new Chunk(chunk.getID(), chunk.getDataSize());
			offset = 0;
		}

		ByteBuffer buffer = chunk.getData();
		try {
			assert buffer != null;
			buffer.position(offset);
		} catch (final IllegalArgumentException e) {
			// set to end
			buffer.position(buffer.capacity());
		}

		dataType = dataType.toLowerCase();
		switch (dataType) {
			case "str":
				byte[] bytes = data.getBytes(StandardCharsets.US_ASCII);

				try {
					int size = buffer.capacity() - buffer.position();
					if (bytes.length < size) {
						size = bytes.length;
					}
					buffer.put(bytes, 0, size);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
				}
				break;
			case "byte":
				byte b = (byte) (new DataTypeParserByte()).parse(data);

				try {
					buffer.put(b);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
				}
				break;
			case "short": {
				short v = (short) (new DataTypeParserShort()).parse(data);

				try {
					buffer.putShort(v);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
				}
				break;
			}
			case "int": {
				int v = (int) (new DataTypeParserInt()).parse(data);

				try {
					buffer.putInt(v);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
				}
				break;
			}
			case "long": {
				long v = (long) (new DataTypeParserLong()).parse(data);

				try {
					buffer.putLong(v);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
				}
				break;
			}
			case "hex":
				DataTypeParserByte parser = new DataTypeParserByte();
				String[] tokens = data.split(" ");

				for (String token : tokens) {
					b = (byte) parser.parse(token);
					try {
						buffer.put(b);
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
						break;
					}
				}
				break;
			default:
				getTerminalDelegate().println("error: Unsupported data type " + dataType, TerminalColor.RED);
				return true;
		}

		// put chunk back
		if (!tmpStorageService.put(chunk)) {
			getTerminalDelegate().println(
					"error: Putting chunk " + ChunkID.toHexString(chunk.getID()) + " into temporary storage failed",
					TerminalColor.RED);
		} else {
			getTerminalDelegate().println(
					"Put to chunk " + ChunkID.toHexString(chunk.getID()) + " to temporary storage successful.");
		}

		return true;
	}
}
