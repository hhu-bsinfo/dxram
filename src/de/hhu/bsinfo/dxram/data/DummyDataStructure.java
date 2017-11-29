package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Zero sized dummy data structure
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class DummyDataStructure extends DataStructure {

    /**
     * Constructor
     *
     * @param p_chunkID
     *     Chunk ID to assign
     */
    public DummyDataStructure(final long p_chunkID) {
        super(p_chunkID);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {

    }

    @Override
    public void importObject(final Importer p_importer) {

    }

    @Override
    public int sizeofObject() {
        return 0;
    }
}
