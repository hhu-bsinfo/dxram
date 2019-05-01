package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.*;
import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.data.ChunkID;
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
public class DirectCityTest {

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
    public void testCreation(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        BootService bootService = p_instance.getService(BootService.class);

        int count = 10;

        final long id = DirectCity.create();
        long addr = DirectCity.getAddress(id);
        Assert.assertEquals(0xFFFF000000000000L, ChunkID.getCreatorID(id) & 0xFFFF000000000000L);
        Assert.assertEquals(addr, DirectCity.getAddress(id));
        Assert.assertEquals(DirectCity.getArea(id), DirectCity.getArea(addr));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(id));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(addr));
        Assert.assertEquals(DirectCity.getName(id), DirectCity.getName(addr));
        Assert.assertEquals(DirectCity.getPopulation(id), DirectCity.getPopulation(addr));

        final long[] ids = DirectCity.create(count);
        for (int i = 0; i < count; i++) {
            addr = DirectCity.getAddress(ids[i]);
            Assert.assertEquals((short) 0xFFFF, ChunkID.getCreatorID(ids[i]));
            Assert.assertEquals(addr, DirectCity.getAddress(ids[i]));
            Assert.assertEquals(DirectCity.getArea(ids[i]), DirectCity.getArea(addr));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(ids[i]));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(addr));
            Assert.assertEquals(DirectCity.getName(ids[i]), DirectCity.getName(addr));
            Assert.assertEquals(DirectCity.getPopulation(ids[i]), DirectCity.getPopulation(addr));
        }

        final long[] reserved = DirectCity.reserve(count);
        DirectCity.createReserved(reserved);
        for (int i = 0; i < count; i++) {
            addr = DirectCity.getAddress(reserved[i]);
            Assert.assertEquals(bootService.getNodeID(), ChunkID.getCreatorID(reserved[i]));
            Assert.assertEquals(addr, DirectCity.getAddress(reserved[i]));
            Assert.assertEquals(DirectCity.getArea(reserved[i]), DirectCity.getArea(addr));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(reserved[i]));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryID(addr));
            Assert.assertEquals(DirectCity.getName(reserved[i]), DirectCity.getName(addr));
            Assert.assertEquals(DirectCity.getPopulation(reserved[i]), DirectCity.getPopulation(addr));
        }

        try (DirectCity city = DirectCity.use(id)) {
            Assert.assertEquals(ChunkID.INVALID_ID, city.getCountryCountryID());
            Assert.assertNull(city.getName());
        } catch(Exception ex) {}
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testRemove(final DXRAM p_instance) {
        for (int i = 0; i < 10; i++) {
            final long chunk = DirectCity.create();
            DirectCity.remove(chunk);
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testStaticGetterAndSetter(final DXRAM p_instance) {
        final long id = DirectCity.create();
        final long address = DirectCity.getAddress(id);

        int area = 11;
        long fakeCountryCID = 932760943L;
        String name = "Ali Baba";
        int population = 2524535;

        // set all (static)
        DirectCity.setArea(id, area);
        DirectCity.setCountryCountryID(id, fakeCountryCID);
        DirectCity.setName(id, name);
        DirectCity.setPopulation(id, population);

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(id));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(id));
        Assert.assertEquals(name, DirectCity.getName(id));
        Assert.assertEquals(population, DirectCity.getPopulation(id));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getArea(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(address));
        Assert.assertEquals(name, DirectCity.getName(address));
        Assert.assertEquals(population, DirectCity.getPopulation(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(id)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }

        // reset all (static)
        area = 324532;
        fakeCountryCID = 508345L;
        name = "Lipton";
        population = 82375325;
        DirectCity.setArea(id, area);
        DirectCity.setCountryCountryID(id, fakeCountryCID);
        DirectCity.setName(id, name);
        DirectCity.setPopulation(id, population);

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(id));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(id));
        Assert.assertEquals(name, DirectCity.getName(id));
        Assert.assertEquals(population, DirectCity.getPopulation(id));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getArea(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(address));
        Assert.assertEquals(name, DirectCity.getName(address));
        Assert.assertEquals(population, DirectCity.getPopulation(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(id)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }

        // reset all (obj. accessor)
        area = 633;
        fakeCountryCID = 8543L;
        name = "Musterstadt";
        population = 32855;
        try (DirectCity city = DirectCity.use(id)) {
            city.setArea(area);
            city.setCountryCountryID(fakeCountryCID);
            city.setName(name);
            city.setPopulation(population);
        }

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(id));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(id));
        Assert.assertEquals(name, DirectCity.getName(id));
        Assert.assertEquals(population, DirectCity.getPopulation(id));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getArea(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryID(address));
        Assert.assertEquals(name, DirectCity.getName(address));
        Assert.assertEquals(population, DirectCity.getPopulation(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(id)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }
    }
}
