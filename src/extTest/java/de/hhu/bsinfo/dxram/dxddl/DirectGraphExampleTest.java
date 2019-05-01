package de.hhu.bsinfo.dxram.dxddl;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.BeforeTestInstance;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.util.NodeRole;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DirectGraphExampleTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        BootService bootService = p_instance.getService(BootService.class);

        DirectAccessSecurityManager.init(
                bootService,
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void test(final DXRAM p_instance) {
        // example-directed from LDBC
        final long graph_id = DirectGraph.create();
        final long[] vertices = DirectVertex.create(10);
        final long[] edges = DirectEdge.create(17);

        // validate types
        Assert.assertTrue(DirectGraph.isValidType(graph_id));
        for (int i = 0; i < vertices.length; i++) {
            Assert.assertTrue(DirectVertex.isValidType(vertices[i]));
        }
        for (int i = 0; i < edges.length; i++) {
            Assert.assertTrue(DirectEdge.isValidType(edges[i]));
        }

        /* set all */

        DirectGraph.setName(graph_id, "example-directed");
        DirectGraph.setVersion(graph_id, 1L);
        DirectGraph.setEdgeListEdgeIDs(graph_id, edges);

        DirectVertex.setDepth(vertices[0], 0);
        DirectVertex.setDepth(vertices[1], -1);
        DirectVertex.setDepth(vertices[2], 1);
        DirectVertex.setDepth(vertices[3], 2);
        DirectVertex.setDepth(vertices[4], 1);
        DirectVertex.setDepth(vertices[5], -1);
        DirectVertex.setDepth(vertices[6], -1);
        DirectVertex.setDepth(vertices[7], 2);
        DirectVertex.setDepth(vertices[8], -1);
        DirectVertex.setDepth(vertices[9], 2);

        DirectVertex.setInNeighborsVertexIDs(vertices[0], new long[] { vertices[2], vertices[7] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[0], new long[] { vertices[2], vertices[4] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[1], new long[] { vertices[3], vertices[4] });
        DirectVertex.setInNeighborsVertexIDs(vertices[2], new long[] { vertices[0], vertices[4], vertices[5] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[2], new long[] { vertices[0], vertices[4], vertices[7], vertices[9] });
        DirectVertex.setInNeighborsVertexIDs(vertices[3], new long[] { vertices[1], vertices[4], vertices[5], vertices[6], vertices[8] });
        DirectVertex.setInNeighborsVertexIDs(vertices[4], new long[] { vertices[0], vertices[1], vertices[2] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[4], new long[] { vertices[2], vertices[3], vertices[7] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[5], new long[] { vertices[2], vertices[3] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[6], new long[] { vertices[3] });
        DirectVertex.setInNeighborsVertexIDs(vertices[7], new long[] { vertices[2], vertices[4] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[7], new long[] { vertices[0] });
        DirectVertex.setOutNeighborsVertexIDs(vertices[8], new long[] { vertices[3] });
        DirectVertex.setInNeighborsVertexIDs(vertices[9], new long[] { vertices[2] });

        DirectEdge.setSrcVertexID(edges[0], vertices[0]);
        DirectEdge.setDstVertexID(edges[0], vertices[2]);
        DirectEdge.setWeight(edges[0], 0.5);
        DirectEdge.setSrcVertexID(edges[1], vertices[0]);
        DirectEdge.setDstVertexID(edges[1], vertices[4]);
        DirectEdge.setWeight(edges[1], 0.3);
        DirectEdge.setSrcVertexID(edges[2], vertices[1]);
        DirectEdge.setDstVertexID(edges[2], vertices[3]);
        DirectEdge.setWeight(edges[2], 0.1);
        DirectEdge.setSrcVertexID(edges[3], vertices[1]);
        DirectEdge.setDstVertexID(edges[3], vertices[4]);
        DirectEdge.setWeight(edges[3], 0.3);
        DirectEdge.setSrcVertexID(edges[4], vertices[2]);
        DirectEdge.setDstVertexID(edges[4], vertices[9]);
        DirectEdge.setWeight(edges[4], 0.12);
        DirectEdge.setSrcVertexID(edges[5], vertices[2]);
        DirectEdge.setDstVertexID(edges[5], vertices[0]);
        DirectEdge.setWeight(edges[5], 0.53);
        DirectEdge.setSrcVertexID(edges[6], vertices[2]);
        DirectEdge.setDstVertexID(edges[6], vertices[4]);
        DirectEdge.setWeight(edges[6], 0.62);
        DirectEdge.setSrcVertexID(edges[7], vertices[2]);
        DirectEdge.setDstVertexID(edges[7], vertices[7]);
        DirectEdge.setWeight(edges[7], 0.21);
        DirectEdge.setSrcVertexID(edges[8], vertices[2]);
        DirectEdge.setDstVertexID(edges[8], vertices[9]);
        DirectEdge.setWeight(edges[8], 0.52);
        DirectEdge.setSrcVertexID(edges[9], vertices[4]);
        DirectEdge.setDstVertexID(edges[9], vertices[2]);
        DirectEdge.setWeight(edges[9], 0.69);
        DirectEdge.setSrcVertexID(edges[10], vertices[4]);
        DirectEdge.setDstVertexID(edges[10], vertices[3]);
        DirectEdge.setWeight(edges[10], 0.53);
        DirectEdge.setSrcVertexID(edges[11], vertices[4]);
        DirectEdge.setDstVertexID(edges[11], vertices[7]);
        DirectEdge.setWeight(edges[11], 0.1);
        DirectEdge.setSrcVertexID(edges[12], vertices[5]);
        DirectEdge.setDstVertexID(edges[12], vertices[2]);
        DirectEdge.setWeight(edges[12], 0.23);
        DirectEdge.setSrcVertexID(edges[13], vertices[5]);
        DirectEdge.setDstVertexID(edges[13], vertices[3]);
        DirectEdge.setWeight(edges[13], 0.39);
        DirectEdge.setSrcVertexID(edges[14], vertices[6]);
        DirectEdge.setDstVertexID(edges[14], vertices[3]);
        DirectEdge.setWeight(edges[14], 0.83);
        DirectEdge.setSrcVertexID(edges[15], vertices[7]);
        DirectEdge.setDstVertexID(edges[15], vertices[0]);
        DirectEdge.setWeight(edges[15], 0.39);
        DirectEdge.setSrcVertexID(edges[16], vertices[8]);
        DirectEdge.setDstVertexID(edges[16], vertices[3]);
        DirectEdge.setWeight(edges[16], 0.69);

        /* test */

        Assert.assertEquals("example-directed", DirectGraph.getName(graph_id));
        Assert.assertEquals(1L, DirectGraph.getVersion(graph_id));
        Assert.assertArrayEquals(edges, DirectGraph.getEdgeListEdgeIDs(graph_id));

        final long graph_cid = DirectGraph.getCID(graph_id);

        Assert.assertEquals("example-directed", DirectGraph.getName(graph_cid));
        Assert.assertEquals(1L, DirectGraph.getVersion(graph_cid));
        Assert.assertArrayEquals(edges, DirectGraph.getEdgeListEdgeIDs(graph_cid));
        Assert.assertArrayEquals(edges, DirectGraph.getEdgeListEdgeLocalIDs(graph_cid));
        Assert.assertArrayEquals(new long[0], DirectGraph.getEdgeListEdgeRemoteIDs(graph_cid));

        Assert.assertEquals(0, DirectVertex.getDepth(vertices[0]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertices[1]));
        Assert.assertEquals(1, DirectVertex.getDepth(vertices[2]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertices[3]));
        Assert.assertEquals(1, DirectVertex.getDepth(vertices[4]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertices[5]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertices[6]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertices[7]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertices[8]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertices[9]));

        final long[] vertex_cids = DirectVertex.getCIDs(vertices);

        Assert.assertEquals(0, DirectVertex.getDepth(vertex_cids[0]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertex_cids[1]));
        Assert.assertEquals(1, DirectVertex.getDepth(vertex_cids[2]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertex_cids[3]));
        Assert.assertEquals(1, DirectVertex.getDepth(vertex_cids[4]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertex_cids[5]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertex_cids[6]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertex_cids[7]));
        Assert.assertEquals(-1, DirectVertex.getDepth(vertex_cids[8]));
        Assert.assertEquals(2, DirectVertex.getDepth(vertex_cids[9]));

        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertices[0], 0));
        Assert.assertEquals(vertices[7], DirectVertex.getInNeighborsVertexID(vertices[0], 1));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertices[0], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertices[0], 1));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertices[1], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertices[1], 1));
        Assert.assertEquals(vertices[0], DirectVertex.getInNeighborsVertexID(vertices[2], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertices[2], 1));
        Assert.assertEquals(vertices[5], DirectVertex.getInNeighborsVertexID(vertices[2], 2));
        Assert.assertEquals(vertices[0], DirectVertex.getOutNeighborsVertexID(vertices[2], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertices[2], 1));
        Assert.assertEquals(vertices[7], DirectVertex.getOutNeighborsVertexID(vertices[2], 2));
        Assert.assertEquals(vertices[9], DirectVertex.getOutNeighborsVertexID(vertices[2], 3));
        Assert.assertEquals(vertices[1], DirectVertex.getInNeighborsVertexID(vertices[3], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertices[3], 1));
        Assert.assertEquals(vertices[5], DirectVertex.getInNeighborsVertexID(vertices[3], 2));
        Assert.assertEquals(vertices[6], DirectVertex.getInNeighborsVertexID(vertices[3], 3));
        Assert.assertEquals(vertices[8], DirectVertex.getInNeighborsVertexID(vertices[3], 4));
        Assert.assertEquals(vertices[0], DirectVertex.getInNeighborsVertexID(vertices[4], 0));
        Assert.assertEquals(vertices[1], DirectVertex.getInNeighborsVertexID(vertices[4], 1));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertices[4], 2));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertices[4], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertices[4], 1));
        Assert.assertEquals(vertices[7], DirectVertex.getOutNeighborsVertexID(vertices[4], 2));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertices[5], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertices[5], 1));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertices[6], 0));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertices[7], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertices[7], 1));
        Assert.assertEquals(vertices[0], DirectVertex.getOutNeighborsVertexID(vertices[7], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertices[8], 0));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertices[9], 0));

        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertex_cids[0], 0));
        Assert.assertEquals(vertices[7], DirectVertex.getInNeighborsVertexID(vertex_cids[0], 1));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertex_cids[0], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertex_cids[0], 1));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertex_cids[1], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertex_cids[1], 1));
        Assert.assertEquals(vertices[0], DirectVertex.getInNeighborsVertexID(vertex_cids[2], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertex_cids[2], 1));
        Assert.assertEquals(vertices[5], DirectVertex.getInNeighborsVertexID(vertex_cids[2], 2));
        Assert.assertEquals(vertices[0], DirectVertex.getOutNeighborsVertexID(vertex_cids[2], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getOutNeighborsVertexID(vertex_cids[2], 1));
        Assert.assertEquals(vertices[7], DirectVertex.getOutNeighborsVertexID(vertex_cids[2], 2));
        Assert.assertEquals(vertices[9], DirectVertex.getOutNeighborsVertexID(vertex_cids[2], 3));
        Assert.assertEquals(vertices[1], DirectVertex.getInNeighborsVertexID(vertex_cids[3], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertex_cids[3], 1));
        Assert.assertEquals(vertices[5], DirectVertex.getInNeighborsVertexID(vertex_cids[3], 2));
        Assert.assertEquals(vertices[6], DirectVertex.getInNeighborsVertexID(vertex_cids[3], 3));
        Assert.assertEquals(vertices[8], DirectVertex.getInNeighborsVertexID(vertex_cids[3], 4));
        Assert.assertEquals(vertices[0], DirectVertex.getInNeighborsVertexID(vertex_cids[4], 0));
        Assert.assertEquals(vertices[1], DirectVertex.getInNeighborsVertexID(vertex_cids[4], 1));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertex_cids[4], 2));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertex_cids[4], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertex_cids[4], 1));
        Assert.assertEquals(vertices[7], DirectVertex.getOutNeighborsVertexID(vertex_cids[4], 2));
        Assert.assertEquals(vertices[2], DirectVertex.getOutNeighborsVertexID(vertex_cids[5], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertex_cids[5], 1));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertex_cids[6], 0));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertex_cids[7], 0));
        Assert.assertEquals(vertices[4], DirectVertex.getInNeighborsVertexID(vertex_cids[7], 1));
        Assert.assertEquals(vertices[0], DirectVertex.getOutNeighborsVertexID(vertex_cids[7], 0));
        Assert.assertEquals(vertices[3], DirectVertex.getOutNeighborsVertexID(vertex_cids[8], 0));
        Assert.assertEquals(vertices[2], DirectVertex.getInNeighborsVertexID(vertex_cids[9], 0));

        for (long edge : edges) {
            final long src = DirectEdge.getSrcVertexID(edge);
            final long dst = DirectEdge.getDstVertexID(edge);
            Assert.assertTrue(contains(DirectVertex.getInNeighborsVertexIDs(dst), src));
            Assert.assertTrue(contains(DirectVertex.getCIDs(DirectVertex.getInNeighborsVertexIDs(dst)), DirectVertex.getCID(src)));
            Assert.assertTrue(contains(DirectVertex.getOutNeighborsVertexIDs(src), dst));
            Assert.assertTrue(contains(DirectVertex.getCIDs(DirectVertex.getOutNeighborsVertexIDs(src)), DirectVertex.getCID(dst)));
        }
    }

    private static boolean contains(final long[] array, final long value) {
        for (long v : array) {
            if (v == value) {
                return true;
            }
        }

        return false;
    }
}
