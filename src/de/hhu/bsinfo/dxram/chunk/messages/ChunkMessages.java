package de.hhu.bsinfo.dxram.chunk.messages;

/**
 * Type and list of subtypes for all chunk messages
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class ChunkMessages {	
	public static final byte TYPE = 10;
	public static final byte SUBTYPE_GET_REQUEST = 1;
	public static final byte SUBTYPE_GET_RESPONSE = 2;
	public static final byte SUBTYPE_PUT_REQUEST = 3;
	public static final byte SUBTYPE_PUT_RESPONSE = 4;
	public static final byte SUBTYPE_REMOVE_REQUEST = 5;
	public static final byte SUBTYPE_REMOVE_RESPONSE = 6;
	public static final byte SUBTYPE_CREATE_REQUEST = 7;
	public static final byte SUBTYPE_CREATE_RESPONSE = 8;
	public static final byte SUBTYPE_STATUS_REQUEST = 9;
	public static final byte SUBTYPE_STATUS_RESPONSE = 10;
	
	/**
	 * Static class
	 */
	private ChunkMessages() {};
}
