package de.hhu.bsinfo.dxram.loader.messages;

import java.util.HashSet;
import java.util.Set;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.loader.ObjectSerializer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncRequestMessage extends Request {
    private byte[] m_loadedJars;

    public SyncRequestMessage() {
        super();
    }

    public SyncRequestMessage(final short p_destination, Set<String> p_loadedJars) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST);
        m_loadedJars = ObjectSerializer.serializeObject(p_loadedJars);
    }

    public HashSet<String> getLoadedJars() {
        return (HashSet<String>) ObjectSerializer.deserializeObject(m_loadedJars, HashSet.class);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_loadedJars);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_loadedJars);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_loadedJars = p_importer.readByteArray(m_loadedJars);
    }
}
