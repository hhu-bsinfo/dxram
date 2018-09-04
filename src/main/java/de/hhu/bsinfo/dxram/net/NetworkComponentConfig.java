package de.hhu.bsinfo.dxram.net;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.ib.IBConfig;
import de.hhu.bsinfo.dxnet.nio.NIOConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the NetworkComponent
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 28.07.2017
 */
@Data
@Accessors(prefix = "m_")
@EqualsAndHashCode(callSuper = false)
@DXRAMComponentConfig.Settings(component = NetworkComponent.class, supportsSuperpeer = true, supportsPeer = true)
public class NetworkComponentConfig extends DXRAMComponentConfig {
    /**
     * Get the core configuration values
     */
    @Expose
    private CoreConfig m_coreConfig = new CoreConfig();

    /**
     * Get the NIO specific configuration values
     */
    @Expose
    private NIOConfig m_nioConfig = new NIOConfig();

    /**
     * Get the IB specific configuration values
     */
    @Expose
    private IBConfig m_ibConfig = new IBConfig();

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return m_coreConfig.verify() && m_nioConfig.verify() && m_ibConfig.verify();
    }
}
