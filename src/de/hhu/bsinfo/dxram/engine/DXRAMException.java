package de.hhu.bsinfo.dxram.engine;

public class DXRAMException extends Exception {
    public DXRAMException(final String p_msg) {
        super(p_msg);
    }

    public DXRAMException(final Exception p_e) {
        super(p_e);
    }

    public DXRAMException(final String p_msg, final Exception p_e) {
        super(p_msg, p_e);
    }
}
