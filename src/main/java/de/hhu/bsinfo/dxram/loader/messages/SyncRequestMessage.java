package de.hhu.bsinfo.dxram.loader.messages;

import java.util.HashSet;
import java.util.Set;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncRequestMessage extends Message {
    private Set<String> m_loadedJars;
    private int m_setSize;

    public SyncRequestMessage() {
        super();
    }

    public SyncRequestMessage(final short p_destination, Set<String> p_loadedJars) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST);
        m_loadedJars = p_loadedJars;
        m_setSize = m_loadedJars.size();
    }

    public Set<String> getLoadedJars() {
        return m_loadedJars;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Integer.BYTES;
        for (String s : m_loadedJars) {
            size += ObjectSizeUtil.sizeofString(s);
        }

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_setSize);
        for (String s : m_loadedJars) {
            p_exporter.writeString(s);
        }
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_setSize = p_importer.readInt(m_setSize);
        Set<String> jars = new HashSet<>();

        for (int i = 0; i < m_setSize; i++) {
            String s = "";
            jars.add(p_importer.readString(s));
        }

        m_loadedJars = jars;
    }
}
