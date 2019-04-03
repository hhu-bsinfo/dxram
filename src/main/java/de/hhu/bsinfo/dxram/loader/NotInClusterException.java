package de.hhu.bsinfo.dxram.loader;

public class NotInClusterException extends Exception {
    public NotInClusterException(String p_info) {
        super(p_info);
    }

    public NotInClusterException() {
        super();
    }

}
