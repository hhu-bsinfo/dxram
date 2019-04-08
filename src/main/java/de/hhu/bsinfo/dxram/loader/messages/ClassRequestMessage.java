package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ClassRequestMessage extends Request {
    @Getter
    private String m_packageName;

    public ClassRequestMessage(){
        super();
    }

    public ClassRequestMessage(final short p_destination, final String p_packageName) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REQUEST);
        m_packageName = p_packageName;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_packageName);
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_packageName);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_packageName = p_importer.readString(m_packageName);
    }
}
