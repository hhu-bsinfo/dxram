package de.hhu.bsinfo.dxram.lookup.tests;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.cache.CacheTree;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.ethnet.NodeID;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Mike on 26/10/16.
 */
public class CacheTreeDeletionTest {

    @Test
    public void testDeletion() {
        short ORDER = 10;

        CacheTree tree = new CacheTree(1000000000, ORDER,0);

        short backups[] = {(short) (Math.random() * 100), (short) (Math.random() * 100), (short) (Math.random() * 100)};
        // -1 for default range dokumentieren!

        // how much cache entries
        int nCaches = 10000000; //+ (int) (Math.random() * 1000000);

        short deleteNode = 0;

        long startInitTime = System.currentTimeMillis();
        // fill the tree with random values within a given range
        for (int i = 0; i < nCaches; i++) {

            //long chunkID = (long) (Math.random() * 10000L);
            long chunkID = i;
            short nodeID = (short) (Math.random() * 20);

//            System.out.println("CID "+ ChunkID.toHexString(chunkID) + " NodeID "+NodeID.toHexString(nodeID));
            tree.cacheChunkID(chunkID, nodeID);

            // randomly chose an element with a ten percent chance to be the node to be removed
            if (i == 0 || (Math.random() < 0.10))
                deleteNode = nodeID;
        }
        long estimatedInitTime = System.currentTimeMillis() - startInitTime;
        System.out.println("Elaspsed Init Cache Time: " + estimatedInitTime);

        long startListTime = System.currentTimeMillis();
        // save the values as list
        ArrayList<CacheTree.CacheNodeElement> lNotDeleted = tree.toList();
        long estimatedListTime = System.currentTimeMillis() - startListTime;
        System.out.println("Elaspsed to List Time: " + estimatedListTime);

        try {
            assertTrue(lNotDeleted.size() - 1 == tree.size());
        } catch (final AssertionError e) {
            System.out.println("Different size in list from tree");
        }

//        System.out.println("Tree Before Deletion: \n" + tree + "\n");

        System.out.println("Deleted " + NodeID.toHexString(deleteNode));

        long startTime = System.currentTimeMillis();
        boolean b = tree.deleteAllCacheEntriesFromNode(deleteNode);
        long estimatedTime = System.currentTimeMillis() - startTime;

        System.out.println("Elaspsed Remove Time: " + estimatedTime);

        ArrayList<CacheTree.CacheNodeElement> lDeleted = tree.toList();


        //System.out.println("Tree After Deleteion: \n" + tree + "\n");


        for (int i = 0; i < lNotDeleted.size(); i++) {

            // Check if all values form the list where nothing was deleted
            // are still in the tree, except the ones deleted



            if (lNotDeleted.get(i).getNodeId() != deleteNode) {

                try {
                    assertTrue(tree.getPrimaryPeer(lNotDeleted.get(i).getChunkId()) == lNotDeleted.get(i).getNodeId());
                } catch( final AssertionError e){
                    System.out.println("AssertTrue Deleted: "+deleteNode +" NodeID " + lNotDeleted.get(i).getNodeId());
                    System.out.println("Error at i "+i+ " ChunkID "+ ChunkID.toHexString(lNotDeleted.get(i).getChunkId())+ " NodeID "+NodeID.toHexString(lNotDeleted.get(i).getNodeId())+"\n");
                }

            }else {

                try {
                    assertFalse(tree.getPrimaryPeer(lNotDeleted.get(i).getChunkId()) == lNotDeleted.get(i).getNodeId());
                }catch( final AssertionError e){
                    System.out.println("AssertFalse Deleted: "+deleteNode +" NodeID " + lNotDeleted.get(i).getNodeId());
                    System.out.println("Error at i "+i+ " ChunkID "+ ChunkID.toHexString(lNotDeleted.get(i).getChunkId())+ " NodeID "+NodeID.toHexString(lNotDeleted.get(i).getNodeId())+"\n");
                }
            }

        }

    }
}
