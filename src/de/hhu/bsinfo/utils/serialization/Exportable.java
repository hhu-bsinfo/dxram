package de.hhu.bsinfo.utils.serialization;

/** Interface defining an object which can be exported/serialized.
 * @author Stefan Nothaas 17.12.15 <stefan.nothaas@hhu.de>
 */
public interface Exportable extends ObjectSize
{
	/** Export/Serialize this object using the provided exporter (target).
	 * 
	 *  Don't call this explicitly. Use an Exporter which is calling this
	 *  method implicitly.
	 * 
	 * @param p_exporter Target to export/serialize the object to.
	 * @param p_size Number of bytes available in the targeting exporter. Check this against
	 * 				 the possible size of your resulting object size and either abort exporting
	 * 				 or cut down the object to stay within the limit.
	 * @return Number of bytes written to the exporter or -1 if an error occurred and exporting was aborted.
	 */
	int exportObject(final Exporter p_exporter, final int p_size);
}
