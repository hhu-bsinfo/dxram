package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxmem.core.Address;
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

        DirectCountry.init(
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());

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
    public void testString(final DXRAM p_instance) {
        final long chunk = DirectCountry.create();
        final long address = DirectCountry.getAddress(chunk);
        final String test = "test";

        Assert.assertNull(DirectCountry.getName(chunk));
        Assert.assertNull(DirectCountry.getNameViaAddress(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertNull(country.getName());
        }

        // set empty string (static)
        DirectCountry.setName(chunk, "");
        Assert.assertEquals("", DirectCountry.getName(chunk));
        Assert.assertEquals("", DirectCountry.getNameViaAddress(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals("", country.getName());
        }

        // set another string (static)
        DirectCountry.setName(chunk, test);
        Assert.assertEquals(test, DirectCountry.getName(chunk));
        Assert.assertEquals(test, DirectCountry.getNameViaAddress(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(test, country.getName());
        }

        // set empty string (obj)
        try (DirectCountry country = DirectCountry.use(chunk)) {
            country.setName("");
            Assert.assertEquals("", country.getName());
        }
        Assert.assertEquals("", DirectCountry.getName(chunk));
        Assert.assertEquals("", DirectCountry.getNameViaAddress(address));

        // set another string (obj)
        try (DirectCountry country = DirectCountry.use(chunk)) {
            country.setName(test);
            Assert.assertEquals(test, country.getName());
        }
        Assert.assertEquals(test, DirectCountry.getName(chunk));
        Assert.assertEquals(test, DirectCountry.getNameViaAddress(address));
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
        Assert.assertEquals(area, DirectCountry.getAreaViaAddress(address));
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
        Assert.assertEquals(area, DirectCountry.getAreaViaAddress(address));

        // set static (via address)
        area++;
        population++;
        DirectCountry.setAreaViaAddress(address, area);
        DirectCountry.setPopulationViaAddress(address, population);
        Assert.assertEquals(area, DirectCountry.getArea(chunk));
        Assert.assertEquals(area, DirectCountry.getAreaViaAddress(address));
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
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityCID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(Address.INVALID, DirectCountry.getCapitalCityAddress(chunk));
        Assert.assertEquals(Address.INVALID, DirectCountry.getCapitalCityAddressViaAddress(address));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLengthViaAddress(address));
        Assert.assertNull(DirectCountry.getCitiesCityCIDs(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityCIDsViaAddress(address));
        Assert.assertNull(DirectCountry.getCitiesCityAddresses(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityAddressesViaAddress(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(ChunkID.INVALID_ID, country.getCapitalCityCID());
            Assert.assertEquals(Address.INVALID, country.getCapitalCityAddress());
            Assert.assertEquals(0, country.getCitiesCityLength());
            Assert.assertNull(country.getCitiesCityCIDs());
            Assert.assertNull(country.getCitiesCityAddresses());
        }

        // set via CID (static)
        DirectCountry.setCapitalCityCID(chunk, capital);
        DirectCountry.setCitiesCityCIDs(chunk, cities);
        Assert.assertEquals(capital, DirectCountry.getCapitalCityCID(chunk));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(DirectCity.getAddress(capital), DirectCountry.getCapitalCityAddress(chunk));
        Assert.assertEquals(DirectCity.getAddress(capital), DirectCountry.getCapitalCityAddressViaAddress(address));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLengthViaAddress(address));
        for (int i = 0; i < cities.length; i++) {
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityCID(chunk, i));
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityCIDViaAddress(address, i));
            Assert.assertEquals(DirectCity.getAddress(cities[i]), DirectCountry.getCitiesCityAddress(chunk, i));
            Assert.assertEquals(
                    DirectCity.getAddress(cities[i]),
                    DirectCountry.getCitiesCityAddressViaAddress(address, i));
        }
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(capital, country.getCapitalCityCID());
            Assert.assertEquals(DirectCity.getAddress(capital), country.getCapitalCityAddress());
            Assert.assertEquals(cities.length, country.getCitiesCityLength());
            Assert.assertArrayEquals(cities, country.getCitiesCityCIDs());
            Assert.assertArrayEquals(DirectCity.getAddresses(cities), country.getCitiesCityAddresses());
            for (int i = 0; i < cities.length; i++) {
                Assert.assertEquals(cities[i], country.getCitiesCityCID(i));
                Assert.assertEquals(DirectCity.getAddress(cities[i]), country.getCitiesCityAddress(i));
            }
        }

        // reset via obj.
        DirectCountry.setCapitalCityCID(chunk, ChunkID.INVALID_ID);
        DirectCountry.setCitiesCityCIDs(chunk, null);
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityCID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(Address.INVALID, DirectCountry.getCapitalCityAddress(chunk));
        Assert.assertEquals(Address.INVALID, DirectCountry.getCapitalCityAddressViaAddress(address));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(0, DirectCountry.getCitiesCityLengthViaAddress(address));
        Assert.assertNull(DirectCountry.getCitiesCityCIDs(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityCIDsViaAddress(address));
        Assert.assertNull(DirectCountry.getCitiesCityAddresses(chunk));
        Assert.assertNull(DirectCountry.getCitiesCityAddressesViaAddress(address));
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(ChunkID.INVALID_ID, country.getCapitalCityCID());
            Assert.assertEquals(Address.INVALID, country.getCapitalCityAddress());
            Assert.assertEquals(0, country.getCitiesCityLength());
            Assert.assertNull(country.getCitiesCityCIDs());
            Assert.assertNull(country.getCitiesCityAddresses());
            country.setCapitalCityCID(capital);
            country.setCitiesCityCIDs(cities);
        }

        Assert.assertEquals(capital, DirectCountry.getCapitalCityCID(chunk));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(DirectCity.getAddress(capital), DirectCountry.getCapitalCityAddress(chunk));
        Assert.assertEquals(DirectCity.getAddress(capital), DirectCountry.getCapitalCityAddressViaAddress(address));
        Assert.assertEquals(capital, DirectCountry.getCapitalCityCIDViaAddress(address));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLength(chunk));
        Assert.assertEquals(cities.length, DirectCountry.getCitiesCityLengthViaAddress(address));
        for (int i = 0; i < cities.length; i++) {
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityCID(chunk, i));
            Assert.assertEquals(cities[i], DirectCountry.getCitiesCityCIDViaAddress(address, i));
            Assert.assertEquals(DirectCity.getAddress(cities[i]), DirectCountry.getCitiesCityAddress(chunk, i));
            Assert.assertEquals(
                    DirectCity.getAddress(cities[i]),
                    DirectCountry.getCitiesCityAddressViaAddress(address, i));
        }
        try (DirectCountry country = DirectCountry.use(chunk)) {
            Assert.assertEquals(capital, country.getCapitalCityCID());
            Assert.assertEquals(DirectCity.getAddress(capital), country.getCapitalCityAddress());
            Assert.assertEquals(cities.length, country.getCitiesCityLength());
            Assert.assertArrayEquals(cities, country.getCitiesCityCIDs());
            Assert.assertArrayEquals(DirectCity.getAddresses(cities), country.getCitiesCityAddresses());
            for (int i = 0; i < cities.length; i++) {
                Assert.assertEquals(cities[i], country.getCitiesCityCID(i));
                Assert.assertEquals(DirectCity.getAddress(cities[i]), country.getCitiesCityAddress(i));
            }
        }
    }
}
