package de.hhu.bsinfo.dxram.data;

/**
 * Extending the normal DataStructure, this interface allows DataStructures within
 * the base structure to be added to put, get or remove operations.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 25.01.16
 *
 */
public interface DataStructureRecursive extends DataStructure {

	/**
	 * Return any substructures from this class to be added to a put operation,
	 * if this class is part of a put operation.
	 * @return Array containing any substructures for the put operation with this class or null.
	 */
	public DataStructure[] substructuresRecursivelyPut();
	
	public int numberSubstructuresRecursivelyPut();
	
	/**
	 * Return any substructures from this class to be added to a get operation,
	 * if this class is part of a get operation.
	 * @return Array containing any substructures for the get operation with this class or null.
	 */
	public DataStructure[] substructuresRecursivelyGet();
	
	public int numberSubstructuresRecursivelyGet();
	
	/**
	 * Return any substructures from this class to be added to a put operation,
	 * if this class is part of a put operation.
	 * @return Array containing any substructures for the remove operation with this class or null.
	 */
	public DataStructure[] substructuresRecursivelyRemove();
	
	public int numberSubstructuresRecursivelyRemove();
}
