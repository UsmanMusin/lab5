import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class DBTesterTest {
    static DBTester dbTester = new DBTester();

    @Before
    public void start(){

       // dbTester.createTableIfNeeded();
    }

    @After
    public void closeAll(){
        try {
            dbTester.connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_test(){
        assertTrue(dbTester.test());
    }

    @Test
    public void viewGroups_test() throws SQLException {
        dbTester.viewGroups();
    }

    @Test
    public void viewItems_test() throws SQLException {
        dbTester.viewItems();
    }

    @Test
    public void viewItemsInGroup_test() throws SQLException {
        dbTester.viewItemsInGroup(2);
        dbTester.viewItemsInGroup("Toyota");
    }

    @Test
    public void getGroupID_test() throws SQLException {
        assertEquals(1, dbTester.getGroupID("Ford"));
    }

    @Test
    public void addItemToGroup_test() throws SQLException {
        assertTrue(dbTester.addItemToGroup("Xtrail","Nissan"));
        assertFalse(dbTester.addItemToGroup("Xtrail","Nissan"));
        assertTrue(dbTester.addItemToGroup("Mustang","Ford"));
        dbTester.viewItems();
    }

    @Test
    public void removeItemFromGroup_test() throws SQLException {
        assertTrue(dbTester.removeItemFromGroup("Focus","Ford"));
        assertTrue(dbTester.removeItemFromGroup("Almera","Nissan"));
        assertFalse(dbTester.removeItemFromGroup("Kalina","Lada"));
        dbTester.viewItems();
    }

    @Test
    public void itemReader_test() throws SQLException {
        dbTester.itemReader();
        dbTester.viewItems();
    }

    @Test
    public void groupReader_test() throws SQLException {
        dbTester.groupReader();
        dbTester.viewGroups();
    }


}