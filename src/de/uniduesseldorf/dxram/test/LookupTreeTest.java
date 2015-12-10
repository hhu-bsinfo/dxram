
package de.uniduesseldorf.dxram.test;

import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.lookup.storage.LookupTree;

import de.uniduesseldorf.utils.config.ConfigurationHandler;

/**
 * Test case for the ChunkID data structures
 * @author Kevin Beineke
 *         03.01.2014
 */
public final class LookupTreeTest {

	// Constants
	private static final short ME = 12345;
	private static final short ORDER = 10;

	// Constructors
	/**
	 * Creates an instance of LookupTreeTest
	 */
	private LookupTreeTest() {}

	// Methods
	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		if (p_arguments.length == 1) {
			evaluate(Integer.parseInt(p_arguments[1]));
		} else {
			System.out.println("Define number of elements");
		}
	}

	/**
	 * Evaluates chosen data structure
	 * @param p_numberOfEntries
	 *            The number of entries that have to be added
	 */
	public static void evaluate(final int p_numberOfEntries) {
		long addMe;

		short creator;
		long lower;
		long upper;
		long timeStart;
		long insert;
		long append;

		long rndm;

		LookupTree chunkIDTree;

		addMe = (long) ME << 48;

		// Initialize DXRAM
		try {
			Core.initialize(ConfigurationHandler.getConfigurationFromFile("config/dxram.config"),
					NodesConfigurationHandler.getConfigurationFromFile("config/nodes.config"));
		} catch (final DXRAMException e1) {
			e1.printStackTrace();
		}

		chunkIDTree = new LookupTree(ORDER);
		chunkIDTree.initRange(0, ME, null);

		// Initialize ranges up to 2^22
		for (int i = 1; i <= Math.pow(2, 22); i++) {
			chunkIDTree.initRange(i * 1000 - 1 + addMe, ME, new short[] {(short) (5 + i), -1, -1});
		}
		creator = 0;
		lower = 1;
		upper = (long) Math.pow(2, 22);
		System.out.println("Start test");
		timeStart = System.currentTimeMillis();

		for (int i = 1; i <= p_numberOfEntries; i++) {
			chunkIDTree.migrateRange(i * 1000 + addMe, i * 1000 + 100 + addMe, (short) 2);
		}
		System.out.println("Appends finished");
		append = System.currentTimeMillis();
		for (int i = 1; i <= p_numberOfEntries; i++) {
			rndm = (long) ((upper - lower) * Math.random() + lower) + addMe;
			creator = (short) (1000 * Math.random());
			chunkIDTree.migrateRange(rndm, rndm + 100, creator);
			if (creator != chunkIDTree.getPrimaryPeer(rndm + 50)) {
				System.out.println("Wrong insertion, " + (rndm + 50 & 0x0000FFFFFFFFFFFFL) + ", " + creator);
			}
		}
		System.out.println("Inserts finished");
		insert = System.currentTimeMillis();
		for (int i = 1; i <= p_numberOfEntries; i++) {
			rndm = (long) (Math.pow(2, 22) * Math.random() + addMe);
			chunkIDTree.getPrimaryPeer(rndm);
		}
		System.out.println("BTree: Append: " + (append - timeStart) + " ms, insert: " + (insert - append) + "ms, get: " + (System.currentTimeMillis() - insert)
				+ "ms");
		if (chunkIDTree.validate()) {
			System.out.println("Number of entries in btree: " + chunkIDTree.size() + ", btree is valid");
		} else {
			System.out.println("Number of entries in btree: " + chunkIDTree.size() + ", btree is not valid");
		}

		/*
		 * Example to examine data structure manually
		 * chunkIDTree.migrateObject(1111 + addMe, (short)1);
		 * for (int i = 1; i <= 10; i++) {
		 * chunkIDTree.migrateObject((i * 1000) + addMe, (short)2);
		 * }
		 * System.out.println(chunkIDTree.toString());
		 * chunkIDTree.migrateObject(1001 + addMe, (short)2);
		 * chunkIDTree.migrateObject(1003 + addMe, (short)5);
		 * chunkIDTree.migrateObject(1002 + addMe, (short)2);
		 * chunkIDTree.migrateObject(8000 + addMe, (short)5);
		 * System.out.println(chunkIDTree.toString());
		 * chunkIDTree.migrateObject(999 + addMe, (short)2);
		 * chunkIDTree.migrateObject(995 + addMe, (short)2);
		 * chunkIDTree.migrateObject(997 + addMe, (short)5);
		 * System.out.println(chunkIDTree.toString());
		 * chunkIDTree.migrateRange(500 + addMe, 1500 + addMe, (short)3);
		 * chunkIDTree.migrateObject(800 + addMe, (short)10);
		 * chunkIDTree.removeObject(500 + addMe);
		 * System.out.println(chunkIDTree.toString());
		 * for (int i = 0; i < 11000; i += 10) {
		 * System.out.println(i + ": " + chunkIDTree.getPrimaryPeer(i) + ", " + chunkIDTree.getBackupPeers(i)[0]);
		 * }
		 */

	}
}
