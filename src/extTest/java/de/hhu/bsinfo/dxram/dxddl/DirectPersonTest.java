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
public class DirectPersonTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        DirectPerson.init(
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
    public void testName(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        final String name = "name";

        Assert.assertNull(DirectPerson.getName(chunk));
        Assert.assertNull(DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getName());
        }

        DirectPerson.setName(chunk, "");
        Assert.assertEquals("", DirectPerson.getName(chunk));
        Assert.assertEquals("", DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals("", person.getName());
        }

        DirectPerson.setName(chunk, null);
        Assert.assertNull(DirectPerson.getName(chunk));
        Assert.assertNull(DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getName());
        }

        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setName("");
        }
        Assert.assertEquals("", DirectPerson.getName(chunk));
        Assert.assertEquals("", DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals("", person.getName());
        }

        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setName(null);
        }
        Assert.assertNull(DirectPerson.getName(chunk));
        Assert.assertNull(DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getName());
        }

        DirectPerson.setName(chunk, name);
        Assert.assertEquals(name, DirectPerson.getName(chunk));
        Assert.assertEquals(name, DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(name, person.getName());
        }

        DirectPerson.setName(chunk, "");
        Assert.assertEquals("", DirectPerson.getName(chunk));
        Assert.assertEquals("", DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals("", person.getName());
        }

        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setName(name);
        }
        Assert.assertEquals(name, DirectPerson.getName(chunk));
        Assert.assertEquals(name, DirectPerson.getNameViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(name, person.getName());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testAge(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        short age = 29;

        Assert.assertEquals(DirectPerson.getAge(chunk), DirectPerson.getAgeViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(DirectPerson.getAge(chunk), person.getAge());
        }

        DirectPerson.setAge(chunk, age);
        Assert.assertEquals(age, DirectPerson.getAge(chunk));
        Assert.assertEquals(age, DirectPerson.getAgeViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(age, person.getAge());
        }

        age++;
        DirectPerson.setAgeViaAddress(address, age);
        Assert.assertEquals(age, DirectPerson.getAge(chunk));
        Assert.assertEquals(age, DirectPerson.getAgeViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(age, person.getAge());
        }

        age++;
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setAge(age);
        }
        Assert.assertEquals(age, DirectPerson.getAge(chunk));
        Assert.assertEquals(age, DirectPerson.getAgeViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(age, person.getAge());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testDateOfBirth(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        long dateOfBirth = 1827932589325L;

        Assert.assertEquals(DirectPerson.getDateOfBirth(chunk), DirectPerson.getDateOfBirthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(DirectPerson.getDateOfBirth(chunk), person.getDateOfBirth());
        }

        DirectPerson.setDateOfBirth(chunk, dateOfBirth);
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirth(chunk));
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(dateOfBirth, person.getDateOfBirth());
        }

        dateOfBirth++;
        DirectPerson.setDateOfBirthViaAddress(address, dateOfBirth);
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirth(chunk));
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(dateOfBirth, person.getDateOfBirth());
        }

        dateOfBirth++;
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setDateOfBirth(dateOfBirth);
        }
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirth(chunk));
        Assert.assertEquals(dateOfBirth, DirectPerson.getDateOfBirthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(dateOfBirth, person.getDateOfBirth());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testHomeAddress(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        String homeAddress = "My Home";

        Assert.assertNull(DirectPerson.getHomeAddress(chunk));
        Assert.assertNull(DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getHomeAddress());
        }

        DirectPerson.setHomeAddress(chunk, homeAddress);
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddress(chunk));
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(homeAddress, person.getHomeAddress());
        }

        DirectPerson.setHomeAddress(chunk, null);
        Assert.assertNull(DirectPerson.getHomeAddress(chunk));
        Assert.assertNull(DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getHomeAddress());
        }

        homeAddress = "My Home 2";
        DirectPerson.setHomeAddressViaAddress(address, homeAddress);
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddress(chunk));
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(homeAddress, person.getHomeAddress());
        }

        DirectPerson.setHomeAddressViaAddress(address, null);
        Assert.assertNull(DirectPerson.getHomeAddress(chunk));
        Assert.assertNull(DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getHomeAddress());
        }

        homeAddress = "My Home 3";
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setHomeAddress(homeAddress);
        }
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddress(chunk));
        Assert.assertEquals(homeAddress, DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(homeAddress, person.getHomeAddress());
        }

        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setHomeAddress(null);
        }
        Assert.assertNull(DirectPerson.getHomeAddress(chunk));
        Assert.assertNull(DirectPerson.getHomeAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getHomeAddress());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testEmail(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        String email = "test@example.com";

        Assert.assertNull(DirectPerson.getEmail(chunk));
        Assert.assertNull(DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getEmail());
        }

        DirectPerson.setEmail(chunk, email);
        Assert.assertEquals(email, DirectPerson.getEmail(chunk));
        Assert.assertEquals(email, DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(email, person.getEmail());
        }

        DirectPerson.setEmail(chunk, null);
        Assert.assertNull(DirectPerson.getEmail(chunk));
        Assert.assertNull(DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getEmail());
        }

        email = "My Home 2";
        DirectPerson.setEmailViaAddress(address, email);
        Assert.assertEquals(email, DirectPerson.getEmail(chunk));
        Assert.assertEquals(email, DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(email, person.getEmail());
        }

        DirectPerson.setEmailViaAddress(address, null);
        Assert.assertNull(DirectPerson.getEmail(chunk));
        Assert.assertNull(DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertNull(person.getEmail());
        }

        email = "My Home 3";
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setEmail(email);
        }
        Assert.assertEquals(email, DirectPerson.getEmail(chunk));
        Assert.assertEquals(email, DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(email, person.getEmail());
        }

        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setEmail("");
        }
        Assert.assertEquals("", DirectPerson.getEmail(chunk));
        Assert.assertEquals("", DirectPerson.getEmailViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals("", person.getEmail());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testPlaceOfBirth(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        final long placeOfBirth = DirectCity.create();

        Assert.assertEquals(ChunkID.INVALID_ID, DirectPerson.getPlaceOfBirthCityCID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectPerson.getPlaceOfBirthCityCIDViaAddress(address));
        Assert.assertEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddress(chunk));
        Assert.assertEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, person.getPlaceOfBirthCityCID());
            Assert.assertEquals(Address.INVALID, person.getPlaceOfBirthCityAddress());
        }

        DirectPerson.setPlaceOfBirthCityCID(chunk, placeOfBirth);
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCID(chunk));
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCIDViaAddress(address));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddress(chunk));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        Assert.assertEquals(
                DirectPerson.getPlaceOfBirthCityAddress(chunk),
                DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(placeOfBirth, person.getPlaceOfBirthCityCID());
            Assert.assertNotEquals(Address.INVALID, person.getPlaceOfBirthCityAddress());
            Assert.assertEquals(
                    DirectPerson.getPlaceOfBirthCityAddress(chunk),
                    person.getPlaceOfBirthCityAddress());
        }

        DirectPerson.setPlaceOfBirthCityCID(chunk, DirectCity.create());
        DirectPerson.setPlaceOfBirthCityCIDViaAddress(address, placeOfBirth);
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCID(chunk));
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCIDViaAddress(address));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddress(chunk));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        Assert.assertEquals(
                DirectPerson.getPlaceOfBirthCityAddress(chunk),
                DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(placeOfBirth, person.getPlaceOfBirthCityCID());
            Assert.assertNotEquals(Address.INVALID, person.getPlaceOfBirthCityAddress());
            Assert.assertEquals(
                    DirectPerson.getPlaceOfBirthCityAddress(chunk),
                    person.getPlaceOfBirthCityAddress());
        }

        DirectPerson.setPlaceOfBirthCityCID(chunk, DirectCity.create());
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setPlaceOfBirthCityCID(placeOfBirth);
        }
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCID(chunk));
        Assert.assertEquals(placeOfBirth, DirectPerson.getPlaceOfBirthCityCIDViaAddress(address));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddress(chunk));
        Assert.assertNotEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        Assert.assertEquals(
                DirectPerson.getPlaceOfBirthCityAddress(chunk),
                DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(placeOfBirth, person.getPlaceOfBirthCityCID());
            Assert.assertNotEquals(Address.INVALID, person.getPlaceOfBirthCityAddress());
            Assert.assertEquals(
                    DirectPerson.getPlaceOfBirthCityAddress(chunk),
                    person.getPlaceOfBirthCityAddress());
        }

        DirectPerson.setPlaceOfBirthCityCID(chunk, ChunkID.INVALID_ID);
        Assert.assertEquals(ChunkID.INVALID_ID, DirectPerson.getPlaceOfBirthCityCID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectPerson.getPlaceOfBirthCityCIDViaAddress(address));
        Assert.assertEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddress(chunk));
        Assert.assertEquals(Address.INVALID, DirectPerson.getPlaceOfBirthCityAddressViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, person.getPlaceOfBirthCityCID());
            Assert.assertEquals(Address.INVALID, person.getPlaceOfBirthCityAddress());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testFavoriteNumbers(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        final int[] favoriteNumbers = new int[] { 3, 5, 7, 11, 13, 17, 19, 23, 27 };
        final int[] favoriteNumbers2 = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };


        Assert.assertNull(DirectPerson.getFavoriteNumbers(chunk));
        Assert.assertNull(DirectPerson.getFavoriteNumbersViaAddress(address));
        Assert.assertEquals(0, DirectPerson.getFamilyPersonLength(chunk));
        Assert.assertEquals(0, DirectPerson.getFamilyPersonLengthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(0, person.getFavoriteNumbersLength());
            Assert.assertNull(person.getFavoriteNumbers());
        }

        DirectPerson.setFavoriteNumbers(chunk, favoriteNumbers);
        Assert.assertEquals(favoriteNumbers.length, DirectPerson.getFavoriteNumbersLength(chunk));
        Assert.assertEquals(favoriteNumbers.length, DirectPerson.getFavoriteNumbersLengthViaAddress(address));
        Assert.assertArrayEquals(favoriteNumbers, DirectPerson.getFavoriteNumbers(chunk));
        Assert.assertArrayEquals(favoriteNumbers, DirectPerson.getFavoriteNumbersViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(favoriteNumbers.length, person.getFavoriteNumbersLength());
            Assert.assertArrayEquals(favoriteNumbers, person.getFavoriteNumbers());
        }
        for (int i = 0; i < favoriteNumbers.length; i++) {
            Assert.assertEquals(favoriteNumbers[i], DirectPerson.getFavoriteNumbers(chunk, i));
            Assert.assertEquals(favoriteNumbers[i], DirectPerson.getFavoriteNumbersViaAddress(address, i));
            try (DirectPerson person = DirectPerson.use(chunk)){
                Assert.assertEquals(favoriteNumbers[i], person.getFavoriteNumbers(i));
            }
        }

        DirectPerson.setFavoriteNumbersViaAddress(address, favoriteNumbers2);
        Assert.assertEquals(favoriteNumbers2.length, DirectPerson.getFavoriteNumbersLength(chunk));
        Assert.assertEquals(favoriteNumbers2.length, DirectPerson.getFavoriteNumbersLengthViaAddress(address));
        Assert.assertArrayEquals(favoriteNumbers2, DirectPerson.getFavoriteNumbers(chunk));
        Assert.assertArrayEquals(favoriteNumbers2, DirectPerson.getFavoriteNumbersViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(favoriteNumbers2.length, person.getFavoriteNumbersLength());
            Assert.assertArrayEquals(favoriteNumbers2, person.getFavoriteNumbers());
        }
        for (int i = 0; i < favoriteNumbers2.length; i++) {
            Assert.assertEquals(favoriteNumbers2[i], DirectPerson.getFavoriteNumbers(chunk, i));
            Assert.assertEquals(favoriteNumbers2[i], DirectPerson.getFavoriteNumbersViaAddress(address, i));
            try (DirectPerson person = DirectPerson.use(chunk)){
                Assert.assertEquals(favoriteNumbers2[i], person.getFavoriteNumbers(i));
            }
        }

        DirectPerson.setFavoriteNumbers(chunk, 3, favoriteNumbers[3]);
        DirectPerson.setFavoriteNumbersViaAddress(address, 1, favoriteNumbers[1]);
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setFavoriteNumbers(0, favoriteNumbers[0]);
        }
        DirectPerson.setFavoriteNumbers(chunk, 4, favoriteNumbers[4]);
        DirectPerson.setFavoriteNumbersViaAddress(address, 5, favoriteNumbers[5]);
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setFavoriteNumbers(2, favoriteNumbers[2]);
        }
        DirectPerson.setFavoriteNumbers(chunk, 6, favoriteNumbers[6]);
        DirectPerson.setFavoriteNumbersViaAddress(address, 7, favoriteNumbers[7]);
        try (DirectPerson person = DirectPerson.use(chunk)){
            person.setFavoriteNumbers(8, favoriteNumbers[8]);
        }
        Assert.assertEquals(favoriteNumbers.length, DirectPerson.getFavoriteNumbersLength(chunk));
        Assert.assertEquals(favoriteNumbers.length, DirectPerson.getFavoriteNumbersLengthViaAddress(address));
        Assert.assertArrayEquals(favoriteNumbers, DirectPerson.getFavoriteNumbers(chunk));
        Assert.assertArrayEquals(favoriteNumbers, DirectPerson.getFavoriteNumbersViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(favoriteNumbers.length, person.getFavoriteNumbersLength());
            Assert.assertArrayEquals(favoriteNumbers, person.getFavoriteNumbers());
        }
        for (int i = 0; i < favoriteNumbers.length; i++) {
            Assert.assertEquals(favoriteNumbers[i], DirectPerson.getFavoriteNumbers(chunk, i));
            Assert.assertEquals(favoriteNumbers[i], DirectPerson.getFavoriteNumbersViaAddress(address, i));
            try (DirectPerson person = DirectPerson.use(chunk)){
                Assert.assertEquals(favoriteNumbers[i], person.getFavoriteNumbers(i));
            }
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testFriendsAndFamily(final DXRAM p_instance) {
        final long chunk = DirectPerson.create();
        final long address = DirectPerson.getAddress(chunk);
        final long[] friends = new long[] { DirectPerson.create(), DirectPerson.create(), DirectPerson.create() };
        final long[] family = new long[] { DirectPerson.create(), DirectPerson.create(), DirectPerson.create() };

        Assert.assertEquals(0, DirectPerson.getFamilyPersonLength(chunk));
        Assert.assertEquals(0, DirectPerson.getFamilyPersonLengthViaAddress(address));
        Assert.assertNull(DirectPerson.getFamilyPersonCIDs(chunk));
        Assert.assertNull(DirectPerson.getFamilyPersonCIDsViaAddress(address));
        Assert.assertNull(DirectPerson.getFamilyPersonAddresses(chunk));
        Assert.assertNull(DirectPerson.getFamilyPersonAddressesViaAddress(address));
        Assert.assertEquals(0, DirectPerson.getFriendsPersonLength(chunk));
        Assert.assertEquals(0, DirectPerson.getFriendsPersonLengthViaAddress(address));
        Assert.assertNull(DirectPerson.getFriendsPersonCIDs(chunk));
        Assert.assertNull(DirectPerson.getFriendsPersonCIDsViaAddress(address));
        Assert.assertNull(DirectPerson.getFriendsPersonAddresses(chunk));
        Assert.assertNull(DirectPerson.getFriendsPersonAddressesViaAddress(address));

        DirectPerson.setFriendsPersonCIDs(chunk, friends);
        DirectPerson.setFamilyPersonCIDsViaAddress(address, family);
        Assert.assertEquals(friends.length, DirectPerson.getFriendsPersonLength(chunk));
        Assert.assertEquals(friends.length, DirectPerson.getFriendsPersonLengthViaAddress(address));
        Assert.assertEquals(family.length, DirectPerson.getFamilyPersonLength(chunk));
        Assert.assertEquals(family.length, DirectPerson.getFamilyPersonLengthViaAddress(address));
        try (DirectPerson person = DirectPerson.use(chunk)){
            Assert.assertEquals(friends.length, person.getFriendsPersonLength());
            Assert.assertEquals(family.length, person.getFamilyPersonLength());
        }
        for (int i = 0; i < friends.length; i++) {
            Assert.assertEquals(friends[i], DirectPerson.getFriendsPersonCID(chunk, i));
            Assert.assertEquals(friends[i], DirectPerson.getFriendsPersonCIDViaAddress(address, i));
        }
        for (int i = 0; i < family.length; i++) {
            Assert.assertEquals(family[i], DirectPerson.getFamilyPersonCID(chunk, i));
            Assert.assertEquals(family[i], DirectPerson.getFamilyPersonCIDViaAddress(address, i));
        }
    }
}
