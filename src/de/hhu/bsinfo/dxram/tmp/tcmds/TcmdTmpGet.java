package de.hhu.bsinfo.dxram.tmp.tcmds;

import java.lang.reflect.InvocationTargetException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.dxram.term.TerminalColor;
import de.hhu.bsinfo.dxram.tmp.TemporaryStorageService;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Get a chunk from the temporary (superpeer) storage.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.05.16
 */
public class TcmdTmpGet extends AbstractTerminalCommand {
	private static final ArgumentList.Argument MS_ARG_ID =
			new ArgumentList.Argument("id", null, false, "Id of the chunk stored in temporary storage");
	private static final ArgumentList.Argument MS_ARG_OFF =
			new ArgumentList.Argument("offset", "0", true, "Offset within the chunk to start getting data from");
	private static final ArgumentList.Argument MS_ARG_LEN = new ArgumentList.Argument("length", null, true,
			"Number of bytes to get starting at the specified offset (end of chunk will be truncated)");
	private static final ArgumentList.Argument MS_ARG_TYPE =
			new ArgumentList.Argument("type", "byte", true, "Format to print the data (str, byte, short, int, long)");
	private static final ArgumentList.Argument MS_ARG_HEX =
			new ArgumentList.Argument("hex", "true", true, "For some representations, print as hex instead of decimal");
	private static final ArgumentList.Argument MS_ARG_CLASS =
			new ArgumentList.Argument("class", null, true,
					"Fully qualified name of the class to get the chunk data to (must be a DataStructure)");

	@Override
	public String getName() {
		return "tmpget";
	}

	@Override
	public String getDescription() {
		return "Get a chunk from the temporary storage";

	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_ID);
		p_arguments.setArgument(MS_ARG_OFF);
		p_arguments.setArgument(MS_ARG_LEN);
		p_arguments.setArgument(MS_ARG_TYPE);
		p_arguments.setArgument(MS_ARG_HEX);
		p_arguments.setArgument(MS_ARG_CLASS);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		Integer id = p_arguments.getArgumentValue(MS_ARG_ID, Integer.class);
		Integer off = p_arguments.getArgumentValue(MS_ARG_OFF, Integer.class);
		Integer len = p_arguments.getArgumentValue(MS_ARG_LEN, Integer.class);
		String dataType = p_arguments.getArgumentValue(MS_ARG_TYPE, String.class);
		boolean hex = p_arguments.getArgumentValue(MS_ARG_HEX, Boolean.class);
		String className = p_arguments.getArgumentValue(MS_ARG_CLASS, String.class);

		TemporaryStorageService tmpStorageService =
				getTerminalDelegate().getDXRAMService(TemporaryStorageService.class);

		if (className != null) {
			Class<?> clazz;
			try {
				clazz = Class.forName(className);
			} catch (final ClassNotFoundException e) {
				getTerminalDelegate().println("Cannot find class with name " + className, TerminalColor.RED);
				return true;
			}

			if (!DataStructure.class.isAssignableFrom(clazz)) {
				getTerminalDelegate()
						.println("Class " + className + " is not implementing the DataStructure interface");
				return true;
			}

			DataStructure dataStructure;
			try {
				dataStructure = (DataStructure) clazz.getConstructor().newInstance();
			} catch (final InstantiationException | IllegalAccessException
					| InvocationTargetException | NoSuchMethodException e) {
				getTerminalDelegate().println("Creating instance of " + className + " failed: " + e.getMessage());
				return false;
			}

			dataStructure.setID(id);

			if (!tmpStorageService.get(dataStructure)) {
				getTerminalDelegate().println(
						"Getting data structure " + ChunkID.toHexString(id) + " from temporary storage failed.",
						TerminalColor.RED);
				return true;
			}

			getTerminalDelegate().println("DataStructure " + className + ": ");
			getTerminalDelegate().println(dataStructure);
		} else {
			Chunk chunk = tmpStorageService.get(id);
			if (chunk == null) {
				getTerminalDelegate()
						.println("Getting chunk " + ChunkID.toHexString(id) + " from temporary storage failed.",
								TerminalColor.RED);
				return true;
			}

			// full length if not specified
			if (len == null) {
				len = chunk.getDataSize();
			}

			ByteBuffer buffer = chunk.getData();
			try {
				assert buffer != null;
				buffer.position(off);
			} catch (final IllegalArgumentException e) {
				// set to end
				buffer.position(buffer.capacity());
			}

			String str = "";
			dataType = dataType.toLowerCase();
			switch (dataType) {
				case "str":
					byte[] bytes = new byte[buffer.capacity() - buffer.position()];

					try {
						buffer.get(bytes, 0, len);
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
					}

					str = new String(bytes, StandardCharsets.US_ASCII);
					break;
				case "byte":
					try {
						for (int i = 0; i < len; i += Byte.BYTES) {
							if (hex) {
								str += Integer.toHexString(buffer.get() & 0xFF) + " ";
							} else {
								str += buffer.get() + " ";
							}
						}
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
					}
					break;
				case "short":
					try {
						for (int i = 0; i < len; i += Short.BYTES) {
							if (hex) {
								str += Integer.toHexString(buffer.getShort() & 0xFFFF) + " ";
							} else {
								str += buffer.getShort() + " ";
							}
						}
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
					}
					break;
				case "int":
					try {
						for (int i = 0; i < len; i += Integer.BYTES) {
							if (hex) {
								str += Integer.toHexString(buffer.getInt()) + " ";
							} else {
								str += buffer.getInt() + " ";
							}
						}
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
					}
					break;
				case "long":
					try {
						for (int i = 0; i < len; i += Long.BYTES) {
							if (hex) {
								str += Long.toHexString(buffer.getLong()) + " ";
							} else {
								str += buffer.getLong() + " ";
							}
						}
					} catch (final BufferOverflowException e) {
						// that's fine, trunc data
					}
					break;
				default:
					getTerminalDelegate().println("error: Unsupported data type " + dataType, TerminalColor.RED);
					return true;
			}

			getTerminalDelegate().println("Chunk data of " + ChunkID.toHexString(id) + ":");
			getTerminalDelegate().println(str);
		}

		return true;
	}
}
