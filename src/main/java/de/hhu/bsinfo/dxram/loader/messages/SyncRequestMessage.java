package de.hhu.bsinfo.dxram.loader.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncRequestMessage extends Request {
    private byte[] m_loadedJars;

    public SyncRequestMessage() {
        super();
    }

    public SyncRequestMessage(final short p_destination, Set<String> p_loadedJars) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_SYNC_REQUEST);
        m_loadedJars = serializeMap(p_loadedJars);
    }

    public Set<String> getLoadedJars() {
        Set<String> map;
        try {
            ByteArrayInputStream fis = new ByteArrayInputStream(m_loadedJars);
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashSet) ois.readObject();
            ois.close();
            fis.close();

            return map;
        } catch (IOException ioe) {
            // todo logger
            return null;
        } catch (ClassNotFoundException c) {
            // todo logger
            return null;
        }
    }

    private byte[] serializeMap(Object p_map) {
        Object o = p_map;

        byte[] yourBytes = null;

        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(o);
            out.flush();
            yourBytes = bos.toByteArray();
        } catch (IOException e) {
            // todo logger
            e.printStackTrace();
        }

        return yourBytes;
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
