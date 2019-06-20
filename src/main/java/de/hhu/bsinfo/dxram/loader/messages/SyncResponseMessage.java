package de.hhu.bsinfo.dxram.loader.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.loader.LoaderJar;

public class SyncResponseMessage extends Message {
    private LoaderJar[] m_loaderJars;
    private int m_mapSize;
    private LoaderJar m_loaderJarBuffer;

    public SyncResponseMessage() {
        super();
    }

    public SyncResponseMessage(final short p_destination, final LoaderJar[] p_loaderJars) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_RESPONSE);
        m_loaderJars = p_loaderJars;
        m_mapSize = p_loaderJars.length;
    }

    public LoaderJar[] getJarByteArrays() {
        return m_loaderJars;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Integer.BYTES;
        for (LoaderJar loaderJar : m_loaderJars) {
            size += loaderJar.sizeofObject();
        }

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_mapSize);
        for (LoaderJar loaderJar : m_loaderJars) {
            loaderJar.exportObject(p_exporter);
        }
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_mapSize = p_importer.readInt(m_mapSize);
        m_loaderJars = new LoaderJar[m_mapSize];

        for (int i = 0; i < m_mapSize; i++) {
            m_loaderJarBuffer = new LoaderJar();
            m_loaderJarBuffer.importObject(p_importer);
            m_loaderJars[i] = m_loaderJarBuffer;
        }
    }
}
