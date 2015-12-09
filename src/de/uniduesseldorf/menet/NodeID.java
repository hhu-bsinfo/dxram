
package de.uniduesseldorf.menet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.uniduesseldorf.utils.Contract;

/**
 * Wrapper class for a NodeID
 * @author Florian Klein
 *         23.07.2013
 */
public final class NodeID {

	// Constants
	public static final short INVALID_ID = -1;

	public static final int MAX_ID = 65535;

	// Constructors
	/**
	 * Creates an instance of NodeID
	 */
	private NodeID() {}

	// Methods
	/**
	 * Checks if the NodeID is valid
	 * @param p_nodeID
	 *            the NodeID
	 */
	public static void check(final short p_nodeID) {
		Contract.check(p_nodeID != INVALID_ID, "invalid NodeID");
	}
	
	/**
	 * Reads a NodeID from DataInput
	 * @param p_input
	 *            the DataInput
	 * @return the read NodeID
	 * @throws IOException
	 *             if the NodeID could not be read
	 */
	public static short readNodeID(final DataInput p_input) throws IOException {
		short ret;

		Contract.checkNotNull(p_input, "no input given");

		ret = p_input.readShort();

		return ret;
	}

	/**
	 * Reads a NodeID from ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @return the read NodeID
	 */
	public static short readNodeID(final ByteBuffer p_buffer) {
		short ret;

		Contract.checkNotNull(p_buffer, "no buffer given");

		ret = p_buffer.getShort();

		return ret;
	}
	
	/**
	 * Writes a NodeID to DataOutput
	 * @param p_output
	 *            the DataOutput
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if the NodeID could not be written
	 */
	public static void writeNodeID(final DataOutput p_output, final short p_nodeID) throws IOException {
		Contract.checkNotNull(p_output, "no output given");

		p_output.writeShort(p_nodeID);
	}

	/**
	 * Writes a NodeID to ByteBuffer
	 * @param p_buffer
	 *            the ByteBuffer
	 * @param p_nodeID
	 *            the NodeID
	 */
	public static void writeNodeID(final ByteBuffer p_buffer, final short p_nodeID) {
		Contract.checkNotNull(p_buffer, "no buffer given");

		p_buffer.putShort(p_nodeID);
	}

}
