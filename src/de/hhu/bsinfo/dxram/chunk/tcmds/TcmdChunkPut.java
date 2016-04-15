
package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserByte;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserInt;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserLong;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserShort;

public class TcmdChunkPut extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID =
			new Argument("cid", null, true, "Full chunk ID of the chunk to put data to");
	private static final Argument MS_ARG_LID =
			new Argument("lid", null, true, "Separate local id part of chunk to put the data to");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Separate node id part of the chunk to put the data to");
	private static final Argument MS_ARG_DATA_TYPE =
			new Argument("type", "str", true, "Type of the data to store (str, byte, short, int, long, hex)");
	private static final Argument MS_ARG_DATA = new Argument("data", null, false, "Data to store");
	private static final Argument MS_ARG_OFFSET =
			new Argument("offset", "-1", true,
					"Offset within the existing to store the new data to. -1 to override existing data");

	@Override
	public String getName() {
		return "chunkput";
	}

	@Override
	public String getDescription() {

		return "Put data in the specified chunk. Either use a full cid or separete nid + lid to specify the chunk id. "
				+ "If no offset is specified, the whole chunk is overwritten with the new data. "
				+ "Otherwise the data is inserted at the starting offset with its length. "
				+ "If the specified data is too long it will be trunced";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_DATA_TYPE);
		p_arguments.setArgument(MS_ARG_DATA);
		p_arguments.setArgument(MS_ARG_OFFSET);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Long cid = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		String dataType = p_arguments.getArgumentValue(MS_ARG_DATA_TYPE, String.class);
		String data = p_arguments.getArgumentValue(MS_ARG_DATA, String.class);
		Integer offset = p_arguments.getArgumentValue(MS_ARG_OFFSET, Integer.class);

		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		long chunkId = -1;
		// we favor full cid
		if (cid != null) {
			chunkId = cid;
		} else {
			if (lid != null) {
				if (nid == null) {
					System.out.println("error: missing nid for lid");
					return false;
				}

				// create cid
				chunkId = ChunkID.getChunkID(nid, lid);
			} else {
				System.out.println("No cid or nid/lid specified.");
				return false;
			}
		}

		// don't allow put of index chunk
		if (ChunkID.getLocalID(chunkId) == 0) {
			System.out.println("Put of index chunk is not allowed.");
			return true;
		}

		Pair<Integer, Chunk[]> chunks = chunkService.get(new long[] {chunkId});
		if (chunks.first() == 0) {
			System.out.println("Getting chunk " + ChunkID.toHexString(chunkId) + " failed.");
			return true;
		}

		Chunk chunk = chunks.second()[0];
		if (offset == -1) {
			// create new chunk
			chunk = new Chunk(chunk.getID(), chunk.getDataSize());
			offset = 0;
		}

		ByteBuffer buffer = chunk.getData();
		try {
			buffer.position(offset);
		} catch (final IllegalArgumentException e) {
			// set to end
			buffer.position(buffer.capacity());
		}

		dataType = dataType.toLowerCase();
		if (dataType.equals("str")) {
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
		} else if (dataType.equals("byte")) {
			byte b = (byte) (new DataTypeParserByte()).parse(data);

			try {
				buffer.put(b);
			} catch (final BufferOverflowException e) {
				// that's fine, trunc data
			}
		} else if (dataType.equals("short")) {
			short v = (short) (new DataTypeParserShort()).parse(data);

			try {
				buffer.putShort(v);
			} catch (final BufferOverflowException e) {
				// that's fine, trunc data
			}
		} else if (dataType.equals("int")) {
			int v = (int) (new DataTypeParserInt()).parse(data);

			try {
				buffer.putInt(v);
			} catch (final BufferOverflowException e) {
				// that's fine, trunc data
			}
		} else if (dataType.equals("long")) {
			long v = (long) (new DataTypeParserLong()).parse(data);

			try {
				buffer.putLong(v);
			} catch (final BufferOverflowException e) {
				// that's fine, trunc data
			}
		} else if (dataType.equals("hex")) {
			DataTypeParserByte parser = new DataTypeParserByte();
			String[] tokens = data.split(" ");

			for (String token : tokens) {
				byte b = (byte) parser.parse(token);
				try {
					buffer.put(b);
				} catch (final BufferOverflowException e) {
					// that's fine, trunc data
					break;
				}
			}
		} else {
			System.out.println("error: Unsupported data type " + dataType);
			return true;
		}

		// put chunk back
		if (chunkService.put(chunk) != 1) {
			System.out.println("error: Putting chunk " + ChunkID.toHexString(chunk.getID()) + " failed");
			return true;
		} else {
			System.out.println("Put to chunk " + ChunkID.toHexString(chunk.getID()) + " successful.");
		}

		return true;
	}
}
