package de.hhu.bsinfo.dxram.migration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.ModuleConfig;

@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class MigrationServiceConfig extends ModuleConfig {

    public MigrationServiceConfig() {
        super(MigrationService.class);
    }

    @Expose
    private int m_workerCount = 16;
}
