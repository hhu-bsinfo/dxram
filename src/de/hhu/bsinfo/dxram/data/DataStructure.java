package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Interface for any kind of data structure that can be stored and read from
 * memory. Implement this with any object you want to put/get from the memory system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public interface DataStructure extends Importable, Exportable {
    /**
     * Get the unique identifier of this data structure.
     *
     * @return Unique identifier.
     */
    long getID();

    /**
     * Set the unique identifier of this data structure.
     *
     * @param p_id
     *     ID to set.
     */
    void setID(long p_id);
}
