package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.loader.LoaderJar;

public class UpdateMessage extends Message {
    @Getter
    private LoaderJar m_loaderJar;

    public UpdateMessage() {
        super();
    }

    public UpdateMessage(final short p_destination, final LoaderJar p_loaderJar) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_UPDATE);
        m_loaderJar = p_loaderJar;
    }
}
