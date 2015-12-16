package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.utils.Endianness;

// status byte as bit field:
// 0 - 4: 	number of chunks sent (if all bits set, additional length field enabled)
// 5 - 6:	length field size 1, 2, 3, 4 if bits 0 - 4 all set
// 7: 		lock aquire flag
public class ChunkMessagesUtils {
	private ChunkMessagesUtils() {
		
	}
	
	public static byte setNumberOfItemsToSend(final byte p_statusCode, final int p_numItems) {
		byte v = p_statusCode;
		
		if (p_numItems < 0x1F) {
			// clear the length field flags too
			v &= ~0x3F; 
			v |= (p_numItems & 0x1F);
		} else {
			v &= ~0x3F;
			// indicate we use a length field
			v |= 0x1F; 
			
			if (p_numItems <= 0xFF) {
				// 0 indicates length field 1 byte
			} else if (p_numItems <= 0xFFFF) {
				v |= (1 << 5);
			} else if (p_numItems <= 0xFFFFFF) {
				v |= (2 << 5);
			} else {
				v |= (3 << 5);
			}
		}
		
		return v;
	}
	
	public static int getNumberOfItemsSent(final byte p_statusCode) {
		int size = p_statusCode & 0x1F;
		
		if ((size & 0x1F) == 0x1F) {
			size = (size >> 5 & 0x3) + 1;
		} 
		
		return size;
	}
	
	public static int getSizeOfAdditionalLengthField(final byte p_statusCode) {
		byte v = p_statusCode;
		int size = 0;
		
		if ((v & 0x1F) == 0x1F) {
			size = (v >> 5 & 0x3) + 1;
		} 
		
		return size;
	}
	
	public static void setNumberOfItemsInMessageBuffer(final byte p_status, final ByteBuffer p_buffer, final int p_numItems) {
		int sizeAdditionalLengthField = getSizeOfAdditionalLengthField(p_status);
		
		switch (sizeAdditionalLengthField) {
			case 0:
				// length already set with status byte
				break;
			case 1:
				p_buffer.put((byte) (p_numItems & 0xFF)); break;
			case 2:
				p_buffer.putShort((short) (p_numItems & 0xFFFF)); break;
			case 3:
				if (Endianness.getEndianness() > 0) {
					p_buffer.putShort((short) ((p_numItems >> 8) & 0xFFFF));
					p_buffer.put((byte) (p_numItems & 0xFF));
				} else {
					p_buffer.put((byte) (p_numItems & 0xFF));
					p_buffer.putShort((short) ((p_numItems >> 8) & 0xFFFF));
				}
				break;
			case 4:
				p_buffer.putInt(p_numItems); break;
			default:
				assert 1 == 2; break;
		}
	}
	
	// buffer needs to be at the proper position already and will be advanced if
	// there is an additional length field
	public static int getNumberOfItemsFromMessageBuffer(final byte p_status, final ByteBuffer p_buffer) {
		int sizeAdditionalLengthField = getSizeOfAdditionalLengthField(p_status);
		int numChunks = 0;
		
		switch (sizeAdditionalLengthField) {
			case 0:
				numChunks = getNumberOfItemsSent(p_status); break;
			case 1:
				numChunks = p_buffer.get(); break;
			case 2:
				numChunks = p_buffer.getShort(); break;
			case 3:
				if (Endianness.getEndianness() > 0) {
					numChunks = ((p_buffer.getShort() & 0xFFFF) << 8) | (p_buffer.get() & 0xFF);
				} else {
					numChunks = ((p_buffer.get() & 0xFF) << 16) | p_buffer.getShort();
				}
				break;
			case 4:
				numChunks = p_buffer.getInt(); break;
			default:
				assert 1 == 2; break;
		}
		
		return numChunks;
	}
	
	// can mean acquire or release lock depending on the message type
	public static byte setLockFlag(final byte p_statusCode, final boolean p_set) {
		byte v = p_statusCode;
		
		if (p_set) {
			v |= (1 << 7);
		} else {
			v &= ~ (1 << 7);
		}
		
		return v;
	}
	
	public static boolean getLockFlag(final byte p_statusCode) {
		return (p_statusCode & (1 << 7)) == 1;
	}
}
