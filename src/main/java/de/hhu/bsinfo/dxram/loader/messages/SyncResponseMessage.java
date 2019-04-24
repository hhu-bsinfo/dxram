package de.hhu.bsinfo.dxram.loader.messages;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class SyncResponseMessage extends Response {
    private byte[] m_jarByteArrays;

    public SyncResponseMessage() {
        super();
    }

    public SyncResponseMessage(final SyncRequestMessage p_request, final HashMap<String, byte[]> p_jarByteArrays) {
        super(p_request, LoaderMessages.SUBTYPE_SYNC_RESPONSE);
        m_jarByteArrays = serializeMap(p_jarByteArrays);
    }

    public HashMap<String, byte[]> getJarByteArrays() {
        HashMap<String, byte[]> map;
        try {
            ByteArrayInputStream fis = new ByteArrayInputStream(m_jarByteArrays);
            ObjectInputStream ois = new ObjectInputStream(fis);
            map = (HashMap) ois.readObject();
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
        }

        return yourBytes;
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
