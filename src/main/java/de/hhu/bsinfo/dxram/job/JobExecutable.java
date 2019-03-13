package de.hhu.bsinfo.dxram.job;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;

@FunctionalInterface
public interface JobExecutable extends Serializable {
    void execute(final ServiceProvider p_serviceAccessor);
}
