package de.hhu.bsinfo.dxram.data;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

public class MessagesDataStructureImExporter implements Importer, Exporter {

	private ByteBuffer m_messageBuffer = null;
	private int m_payloadSize = 0;
	
	public MessagesDataStructureImExporter(final ByteBuffer p_messageBuffer) {
		m_messageBuffer = p_messageBuffer;
	}
	
	/**
	 * Set the size of the payload to analyze when importing an object 
	 * (for dynamic sized objects) or the amount of bytes in the buffer available
	 * when exporting an object.
	 * Use case: Generic chunk data with dynamic size (see Chunk object).
	 * @param p_size Payload size to set.
	 */
	public void setPayloadSize(final int p_size) {
		m_payloadSize = p_size;
	}
	
	@Override
	public int exportObject(Exportable p_object) {
		return p_object.exportObject(this, m_payloadSize);
	}

	@Override
	public void writeByte(byte p_v) {
		m_messageBuffer.put(p_v);
	}

	@Override
	public void writeShort(short p_v) {
		m_messageBuffer.putShort(p_v);
	}

	@Override
	public void writeInt(int p_v) {
		m_messageBuffer.putInt(p_v);
	}

	@Override
	public void writeLong(long p_v) {
		m_messageBuffer.putLong(p_v);
	}

	@Override
	public void writeFloat(float p_v) {
		m_messageBuffer.putFloat(p_v);
	}

	@Override
	public void writeDouble(double p_v) {
		m_messageBuffer.putDouble(p_v);
	}

	@Override
	public int writeBytes(byte[] p_array) {
		return writeBytes(p_array, 0, p_array.length);
	}

	@Override
	public int writeBytes(byte[] p_array, int p_offset, int p_length) {
		int size = p_length;
		
		if (size > m_messageBuffer.remaining()) {
			size = m_messageBuffer.remaining();
		}
		
		m_messageBuffer.put(p_array, p_offset, size);
		return size;
	}

	@Override
	public int importObject(Importable p_object) {
		return p_object.importObject(this, m_payloadSize);
	}

	@Override
	public byte readByte() {
		return m_messageBuffer.get();
	}

	@Override
	public short readShort() {
		return m_messageBuffer.getShort();
	}

	@Override
	public int readInt() {
		return m_messageBuffer.getInt();
	}

	@Override
	public long readLong() {
		return m_messageBuffer.getLong();
	}

	@Override
	public float readFloat() {
		return m_messageBuffer.getFloat();
	}

	@Override
	public double readDouble() {
		return m_messageBuffer.getDouble();
	}

	@Override
	public int readBytes(byte[] p_array) {
		return readBytes(p_array, 0, p_array.length);
	}

	@Override
	public int readBytes(byte[] p_array, int p_offset, int p_length) {
		int size = p_length;
		
		if (size > m_messageBuffer.remaining()) {
			size = m_messageBuffer.remaining();
		}
		
		m_messageBuffer.get(p_array, p_offset, size);
		return size;
	}

}
