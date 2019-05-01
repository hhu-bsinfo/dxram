package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.*;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import org.junit.Assert;
import org.junit.runner.RunWith;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DirectCountryTest {

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
    public void testString(final DXRAM p_instance) {
        final long chunk = DirectCountry.create();
        final long address = DirectCountry.getAddress(chunk);
        final String test = "test";

        Assert.assertNull(DirectCountry.getName(chunk));
        Assert.assertNull(DirectCountry.getName(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertNull(country.getName());
        }

        // set empty string (static)
        DirectCountry.setName(chunk, "");
        Assert.assertEquals("", DirectCountry.getName(chunk));
        Assert.assertEquals("", DirectCountry.getName(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals("", country.getName());
        }

        // set another string (static)
        DirectCountry.setName(chunk, test);
        Assert.assertEquals(test, DirectCountry.getName(chunk));
        Assert.assertEquals(test, DirectCountry.getName(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(test, country.getName());
        }

        // set empty string (obj)
        try (DirectCountry country = DirectCountry.use(chunk)) {
            country.setName("");
            Assert.assertEquals("", country.getName());
        }
        Assert.assertEquals("", DirectCountry.getName(chunk));
        Assert.assertEquals("", DirectCountry.getName(address));

        // set another string (obj)
        try (DirectCountry country = DirectCountry.use(chunk)) {
            country.setName(test);
            Assert.assertEquals(test, country.getName());
        }
        Assert.assertEquals(test, DirectCountry.getName(chunk));
        Assert.assertEquals(test, DirectCountry.getName(address));
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testPrimitiveDataTypes(final DXRAM p_instance) {
        final long chunk = DirectCountry.create();
        final long address = DirectCountry.getAddress(chunk);

        long population = 3424438;
        int area = 324;

        // set static
        DirectCountry.setArea(chunk, area);
        DirectCountry.setPopulation(chunk, population);
        Assert.assertEquals(area, DirectCountry.getArea(chunk));
        Assert.assertEquals(area, DirectCountry.getArea(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(area, country.getArea());
            Assert.assertEquals(population, country.getPopulation());
        }

        // set obj.
        area++;
        population++;
        try (DirectCountry country = DirectCountry.use(chunk)) {
            country.setArea(area);
            country.setPopulation(population);
            Assert.assertEquals(area, country.getArea());
            Assert.assertEquals(population, country.getPopulation());
        }
        Assert.assertEquals(area, DirectCountry.getArea(chunk));
        Assert.assertEquals(area, DirectCountry.getArea(address));

        // set static (via address)
        area++;
        population++;
        DirectCountry.setArea(address, area);
        DirectCountry.setPopulation(address, population);
        Assert.assertEquals(area, DirectCountry.getArea(chunk));
        Assert.assertEquals(area, DirectCountry.getArea(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(area, country.getArea());
            Assert.assertEquals(population, country.getPopulation());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testChunkReferences(final DXRAM p_instance) {
        final long chunk = DirectCountry.create();
        final long address = DirectCountry.getAddress(chunk);

        final long capital = DirectCity.create();
        final long[] cities = DirectCity.create(10);

        // test default values
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(address));
        Assert.assertNull(DirectCountry.getCitiesCityIDs(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityIDs(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(ChunkID.INVALID_ID, country.getCapitalCityID());
            Assert.assertEquals(0, country.getCitiesCityLength());
            Assert.assertNull(country.getCitiesCityIDs());
        }

        // set via CID (static)
        DirectCountry.setCapitalCityID(chunk, capital);
        DirectCountry.setCitiesCityIDs(chunk, cities);
        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(chunk));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(address));
        for (int i = 0; i < cities.length; i++) {
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityID(chunk, i));
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityID(address, i));
        }
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(capital, country.getCapitalCityID());
            Assert.assertEquals(cities.length, country.getCitiesCityLength());
            Assert.assertArrayEquals(cities, country.getCitiesCityIDs());
            for (int i = 0; i < cities.length; i++) {
                Assert.assertEquals(cities[i], country.getCitiesCityID(i));
            }
        }

        // reset via obj.
        DirectCountry.setCapitalCityID(chunk, ChunkID.INVALID_ID);
        DirectCountry.setCitiesCityIDs(chunk, null);
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(address));
        Assert.assertNull(DirectCountry.getCitiesCityIDs(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityIDs(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(ChunkID.INVALID_ID, country.getCapitalCityID());
            Assert.assertEquals(0, country.getCitiesCityLength());
            Assert.assertNull(country.getCitiesCityIDs());
            country.setCapitalCityID(capital);
            country.setCitiesCityIDs(cities);
        }

        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(chunk));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityID(address));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(address));
        for (int i = 0; i < cities.length; i++) {
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityID(chunk, i));
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityID(address, i));
        }
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(capital, country.getCapitalCityID());
            Assert.assertEquals(cities.length, country.getCitiesCityLength());
            Assert.assertArrayEquals(cities, country.getCitiesCityIDs());
            for (int i = 0; i < cities.length; i++) {
                Assert.assertEquals(cities[i], country.getCitiesCityID(i));
            }
        }
    }
}
