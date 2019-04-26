package de.hhu.bsinfo.dxram.loader.messages;

import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncResponseMessage extends Message {
    private HashMap<String, byte[]> m_jarByteArrays;
    private int m_mapSize;
    private String m_stringBuffer;
    private byte[] m_byteBuffer;

    public SyncResponseMessage() {
        super();
        m_jarByteArrays = new HashMap<>();
    }

    public SyncResponseMessage(final short p_destination, final HashMap<String, byte[]> p_jarByteArrays) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE);
        m_jarByteArrays = p_jarByteArrays;
        m_mapSize = m_jarByteArrays.size();
    }

    public HashMap<String, byte[]> getJarByteArrays() {
        return m_jarByteArrays;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Integer.BYTES;
        for (Map.Entry<String, byte[]> e : m_jarByteArrays.entrySet()) {
            size += ObjectSizeUtil.sizeofString(e.getKey());
            size += ObjectSizeUtil.sizeofByteArray(e.getValue());
        }

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_mapSize);
        for (Map.Entry<String, byte[]> e : m_jarByteArrays.entrySet()) {
            p_exporter.writeString(e.getKey());
            p_exporter.writeByteArray(e.getValue());
        }
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_mapSize = p_importer.readInt(m_mapSize);

        for (int i = 0; i < m_mapSize; i++) {
            m_stringBuffer = p_importer.readString(m_stringBuffer);
            m_byteBuffer = p_importer.readByteArray(m_byteBuffer);
            m_jarByteArrays.put(m_stringBuffer, m_byteBuffer);
        }
    }
}
