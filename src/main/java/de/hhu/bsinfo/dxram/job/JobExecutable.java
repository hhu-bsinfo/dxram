package de.hhu.bsinfo.dxram.job;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

@FunctionalInterface
public interface JobExecutable extends Serializable {
    void execute(final DXRAMServiceAccessor p_serviceAccessor);
}
