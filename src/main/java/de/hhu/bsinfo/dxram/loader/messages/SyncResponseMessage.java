package de.hhu.bsinfo.dxram.loader.messages;

import java.util.HashMap;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.loader.ObjectSerializer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncResponseMessage extends Response {
    private byte[] m_jarByteArrays;

    public SyncResponseMessage() {
        super();
    }

    public SyncResponseMessage(final SyncRequestMessage p_request, final HashMap<String, byte[]> p_jarByteArrays) {
        super(p_request, LoaderMessages.SUBTYPE_SYNC_RESPONSE);
        m_jarByteArrays = ObjectSerializer.serializeObject(p_jarByteArrays);
    }

    public HashMap<String, byte[]> getJarByteArrays() {
        return (HashMap<String, byte[]>) ObjectSerializer.deserializeObject(m_jarByteArrays, HashMap.class);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_jarByteArrays);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_jarByteArrays);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_jarByteArrays = p_importer.readByteArray(m_jarByteArrays);
    }
}
