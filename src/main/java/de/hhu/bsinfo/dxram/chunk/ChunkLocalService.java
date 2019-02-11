package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.GetLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMModuleConfig;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Special service with optimized local only operations (does not work with remotely stored chunks)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ChunkLocalService extends AbstractDXRAMService<DXRAMModuleConfig> {
    // component dependencies
    private AbstractBootComponent m_boot;
    private BackupComponent m_backup;
    private ChunkComponent m_chunk;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;
    private NameserviceComponent m_nameservice;

    // chunk operations of service
    private CreateLocal m_createLocal;
    private GetLocal m_getLocal;
    private ReserveLocal m_reserveLocal;

    /**
     * Get the createLocal operation
     *
     * @return Operation
     */
    public CreateLocal createLocal() {
        return m_createLocal;
    }

    /**
     * Get the getLocal operation
     *
     * @return Operation
     */
    public GetLocal getLocal() {
        return m_getLocal;
    }

    /**
     * Get the ReserveLocal operation
     *
     * @return Operation
     */
    public ReserveLocal reserveLocal() {
        return m_reserveLocal;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_backup = p_componentAccessor.getComponent(BackupComponent.class);
        m_chunk = p_componentAccessor.getComponent(ChunkComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
        m_nameservice = p_componentAccessor.getComponent(NameserviceComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_createLocal = new CreateLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_getLocal = new GetLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);
        m_reserveLocal = new ReserveLocal(getClass(), m_boot, m_backup, m_chunk, m_network, m_lookup, m_nameservice);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }
}
