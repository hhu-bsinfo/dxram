package de.hhu.bsinfo.dxram.management;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;

@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
public class ManagementServiceConfig extends DXRAMModuleConfig {

    public ManagementServiceConfig() {
        super(ManagementService.class);
    }

    @Expose
    private String[] m_endpoints = {"members"};

    @Expose
    private short m_port = 8081;

    @Expose
    private String m_tmpDir = "tmp";
}
