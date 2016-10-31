package de.hhu.bsinfo.utils.serialization;

/**
 * Interface defining an object which can be exported/serialized.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.12.15
 */
public interface Exportable extends ObjectSize {
    /**
     * Export/Serialize this object using the provided exporter (target).
     * Don't call this explicitly. Use an Exporter which is calling this
     * method implicitly.
     *
     * @param p_exporter
     *         Target to export/serialize the object to.
     */
    void exportObject(Exporter p_exporter);
}
