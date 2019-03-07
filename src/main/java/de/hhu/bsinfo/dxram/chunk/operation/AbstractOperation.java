package de.hhu.bsinfo.dxram.chunk.operation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

/**
 * Base class for any key-value store related operation. Splitting per operation enforces a clean structure and avoids
 * large classes (e.g. old ChunkService class) which are hard to maintain.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
class AbstractOperation {
    protected Logger m_logger;
    protected AbstractBootComponent m_boot;
    protected BackupComponent m_backup;
    protected ChunkComponent m_chunk;
    protected NetworkComponent m_network;
    protected LookupComponent m_lookup;
    protected NameserviceComponent m_nameservice;

    /**
     * Constructor
     *
     * @param p_parentService
     *         Instance of parent service this operation belongs to
     * @param p_boot
     *         Instance of BootComponent
     * @param p_backup
     *         Instance of BackupComponent
     * @param p_chunk
     *         Instance of ChunkComponent
     * @param p_network
     *         Instance of NetworkComponent
     * @param p_lookup
     *         Instance of LookupComponent
     * @param p_nameservice
     *         Instance of NameserviceComponent
     */
    AbstractOperation(final Class<? extends AbstractDXRAMService> p_parentService,
            final AbstractBootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        m_logger = LogManager.getFormatterLogger(p_parentService);
        m_boot = p_boot;
        m_backup = p_backup;
        m_chunk = p_chunk;
        m_network = p_network;
        m_lookup = p_lookup;
        m_nameservice = p_nameservice;
    }
}
