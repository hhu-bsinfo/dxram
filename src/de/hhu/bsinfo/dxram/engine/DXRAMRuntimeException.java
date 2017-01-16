package de.hhu.bsinfo.dxram.engine;

public class DXRAMRuntimeException extends RuntimeException {
    public DXRAMRuntimeException(final String p_msg) {
        super(p_msg);
    }

    public DXRAMRuntimeException(final Exception p_e) {
        super(p_e);
    }

    public DXRAMRuntimeException(final String p_msg, final Exception p_e) {
        super(p_msg, p_e);
    }
}
