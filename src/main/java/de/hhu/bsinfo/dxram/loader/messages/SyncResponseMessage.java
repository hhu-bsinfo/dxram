package de.hhu.bsinfo.dxram.loader.messages;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.util.Loader;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.loader.LoaderJar;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncResponseMessage extends Message {
    private HashMap<String, LoaderJar> m_loaderJars;
    private int m_mapSize;
    private String m_stringBuffer;
    private LoaderJar m_loaderJarBuffer;

    public SyncResponseMessage() {
        super();
        m_loaderJars = new HashMap<>();
        m_loaderJarBuffer = new LoaderJar();
    }

    public SyncResponseMessage(final short p_destination, final HashMap<String, LoaderJar> p_loaderJars) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE);
        m_loaderJars = p_loaderJars;
        m_mapSize = p_loaderJars.size();
    }

    public HashMap<String, LoaderJar> getJarByteArrays() {
        return m_loaderJars;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Integer.BYTES;
        for (Map.Entry<String, LoaderJar> e : m_loaderJars.entrySet()) {
            size += ObjectSizeUtil.sizeofString(e.getKey());
            size += e.getValue().sizeofObject();
        }

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_mapSize);
        for (Map.Entry<String, LoaderJar> e : m_loaderJars.entrySet()) {
            p_exporter.writeString(e.getKey());
            e.getValue().exportObject(p_exporter);
        }
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_mapSize = p_importer.readInt(m_mapSize);

        for (int i = 0; i < m_mapSize; i++) {
            m_stringBuffer = p_importer.readString(m_stringBuffer);
            m_loaderJarBuffer.importObject(p_importer);
            m_loaderJars.put(m_stringBuffer, m_loaderJarBuffer);
        }
    }
}
