package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/**
 * Minimal job to test the JobService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobTest extends AbstractJob {
    static final long TEST_CHUNK_ID = 0x4242424242424242L;
    static final String TEST_CHUNK_NAME = "JBTST";

    public JobTest() {

    }

    @Override
    public void execute() {
        NameserviceService nameService = getService(NameserviceService.class);
        nameService.register(TEST_CHUNK_ID, TEST_CHUNK_NAME);
    }
}
