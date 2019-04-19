package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.*;
import org.junit.Assert;
import org.junit.runner.RunWith;

import de.hhu.bsinfo.dxmem.core.Address;
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

        DirectCity.init(
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

        final long chunk = DirectCity.create();
        long addr = chunkLocalService.pinningLocal().translate(chunk);
        Assert.assertEquals(bootService.getNodeID(), ChunkID.getCreatorID(chunk));
        Assert.assertEquals(addr, DirectCity.getAddress(chunk));
        Assert.assertEquals(DirectCity.getArea(chunk), DirectCity.getAreaViaAddress(addr));
        Assert.assertEquals(
                DirectCity.getCountryCountryAddress(chunk),
                DirectCity.getCountryCountryAddressViaAddress(addr));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCIDViaAddress(addr));
        Assert.assertEquals(DirectCity.getName(chunk), DirectCity.getNameViaAddress(addr));
        Assert.assertEquals(DirectCity.getPopulation(chunk), DirectCity.getPopulationViaAddress(addr));

        final long[] chunks = DirectCity.create(count);
        for (int i = 0; i < count; i++) {
            addr = chunkLocalService.pinningLocal().translate(chunks[i]);
            Assert.assertEquals(bootService.getNodeID(), ChunkID.getCreatorID(chunks[i]));
            Assert.assertEquals(addr, DirectCity.getAddress(chunks[i]));
            Assert.assertEquals(DirectCity.getArea(chunks[i]), DirectCity.getAreaViaAddress(addr));
            Assert.assertEquals(Address.INVALID, DirectCity.getCountryCountryAddress(chunks[i]));
            Assert.assertEquals(Address.INVALID, DirectCity.getCountryCountryAddressViaAddress(addr));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCID(chunks[i]));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCIDViaAddress(addr));
            Assert.assertEquals(DirectCity.getName(chunks[i]), DirectCity.getNameViaAddress(addr));
            Assert.assertEquals(DirectCity.getPopulation(chunks[i]), DirectCity.getPopulationViaAddress(addr));
        }

        final long[] reserved = chunkLocalService.reserveLocal().reserve(count);
        DirectCity.createReserved(reserved);
        for (int i = 0; i < count; i++) {
            addr = chunkLocalService.pinningLocal().translate(reserved[i]);
            // TODO: clarify why NID is 0 when create reserved cids
            Assert.assertEquals(0, ChunkID.getCreatorID(reserved[i]));
            Assert.assertEquals(addr, DirectCity.getAddress(reserved[i]));
            Assert.assertEquals(DirectCity.getArea(reserved[i]), DirectCity.getAreaViaAddress(addr));
            Assert.assertEquals(Address.INVALID, DirectCity.getCountryCountryAddress(reserved[i]));
            Assert.assertEquals(Address.INVALID, DirectCity.getCountryCountryAddressViaAddress(addr));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCID(reserved[i]));
            Assert.assertEquals(ChunkID.INVALID_ID, DirectCity.getCountryCountryCIDViaAddress(addr));
            Assert.assertEquals(DirectCity.getName(reserved[i]), DirectCity.getNameViaAddress(addr));
            Assert.assertEquals(DirectCity.getPopulation(reserved[i]), DirectCity.getPopulationViaAddress(addr));
        }

        try (DirectCity city = DirectCity.use(chunk)) {
            Assert.assertEquals(Address.INVALID, city.getCountryCountryAddress());
            Assert.assertEquals(ChunkID.INVALID_ID, city.getCountryCountryCID());
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
        final long chunk = DirectCity.create();
        final long address = DirectCity.getAddress(chunk);

        int area = 11;
        long fakeCountryCID = 932760943L;
        String name = "Ali Baba";
        int population = 2524535;

        // set all (static)
        DirectCity.setArea(chunk, area);
        DirectCity.setCountryCountryCID(chunk, fakeCountryCID);
        DirectCity.setName(chunk, name);
        DirectCity.setPopulation(chunk, population);

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(chunk));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCID(chunk));
        Assert.assertEquals(name, DirectCity.getName(chunk));
        Assert.assertEquals(population, DirectCity.getPopulation(chunk));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getAreaViaAddress(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCIDViaAddress(address));
        Assert.assertEquals(name, DirectCity.getNameViaAddress(address));
        Assert.assertEquals(population, DirectCity.getPopulationViaAddress(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(chunk)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryCID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }

        // reset all (static)
        area = 324532;
        fakeCountryCID = 508345L;
        name = "Lipton";
        population = 82375325;
        DirectCity.setArea(chunk, area);
        DirectCity.setCountryCountryCID(chunk, fakeCountryCID);
        DirectCity.setName(chunk, name);
        DirectCity.setPopulation(chunk, population);

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(chunk));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCID(chunk));
        Assert.assertEquals(name, DirectCity.getName(chunk));
        Assert.assertEquals(population, DirectCity.getPopulation(chunk));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getAreaViaAddress(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCIDViaAddress(address));
        Assert.assertEquals(name, DirectCity.getNameViaAddress(address));
        Assert.assertEquals(population, DirectCity.getPopulationViaAddress(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(chunk)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryCID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }

        // reset all (obj. accessor)
        area = 633;
        fakeCountryCID = 8543L;
        name = "Musterstadt";
        population = 32855;
        try (DirectCity city = DirectCity.use(chunk)) {
            city.setArea(area);
            city.setCountryCountryCID(fakeCountryCID);
            city.setName(name);
            city.setPopulation(population);
        }

        // test static getter by CID
        Assert.assertEquals(area, DirectCity.getArea(chunk));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCID(chunk));
        Assert.assertEquals(name, DirectCity.getName(chunk));
        Assert.assertEquals(population, DirectCity.getPopulation(chunk));

        // test static getter by address
        Assert.assertEquals(area, DirectCity.getAreaViaAddress(address));
        Assert.assertEquals(fakeCountryCID, DirectCity.getCountryCountryCIDViaAddress(address));
        Assert.assertEquals(name, DirectCity.getNameViaAddress(address));
        Assert.assertEquals(population, DirectCity.getPopulationViaAddress(address));

        // test getter by object accessor
        try (DirectCity city = DirectCity.use(chunk)) {
            Assert.assertEquals(area, city.getArea());
            Assert.assertEquals(fakeCountryCID, city.getCountryCountryCID());
            Assert.assertEquals(name, city.getName());
            Assert.assertEquals(population, city.getPopulation());
        }
    }
}
