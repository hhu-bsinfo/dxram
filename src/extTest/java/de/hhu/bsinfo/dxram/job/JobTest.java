package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/**
 * Minimal job to test the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobTest extends AbstractJob {
    public static final long TEST_CHUNK_ID = 0x4242424242424242L;
    public static final String TEST_CHUNK_NAME = "JBTST";

    public static final short MS_TYPE_ID = 1;

    public JobTest() {

    }

    @Override
    public short getTypeID() {
        return MS_TYPE_ID;
    }

    @Override
    public void execute() {
        NameserviceService nameService = getService(NameserviceService.class);
        nameService.register(TEST_CHUNK_ID, TEST_CHUNK_NAME);
    }
}
