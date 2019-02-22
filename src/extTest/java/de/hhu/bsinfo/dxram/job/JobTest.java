package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Minimal job to test the JobService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobTest extends AbstractJob {
    static final long TEST_CHUNK_ID = 0x4242424242424242L;
    static final String TEST_CHUNK_NAME = "JBTST";

    private long m_value;

    public JobTest() {

    }

    public JobTest(long p_value) {
        m_value = p_value;
    }

    @Override
    public void execute() {
        BootService bootService = getService(BootService.class);
        System.out.printf("[%04X] %d\n", bootService.getNodeID(), m_value);
        NameserviceService nameService = getService(NameserviceService.class);
        nameService.register(TEST_CHUNK_ID, TEST_CHUNK_NAME);
    }

    @Override
    public void importObject(Importer p_importer) {
        super.importObject(p_importer);
        m_value = p_importer.readLong(m_value);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        super.exportObject(p_exporter);
        p_exporter.writeLong(m_value);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Long.BYTES;
    }
}
