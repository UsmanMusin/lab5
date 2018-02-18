import com.sun.org.apache.regexp.internal.RE;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBTester {
    Connection connection;
    Set <String> addSet = new HashSet<String>();
    Set <String> removeSet = new HashSet<String>();
    DBTester() {
        this.connection = this.ConnectToDB();
    }

    public Connection ConnectToDB() {
        String driver = "org.hsqldb.jdbc.JDBCDriver";
        String url = "jdbc:hsqldb:file:groupdb";
        String user = "SA";
        String pass = "";
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, pass);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e){
            e.printStackTrace();
        }
        return connection;
    }

    public void doWork() throws SQLException{

    }

    public boolean test(){
        Connection con = ConnectToDB();
        try{
            doWork();
            con.close();
            return true;
        }
        catch (SQLException e){
            e.printStackTrace();
            return false;
        }
    }

    public void viewGroups() throws SQLException {
        String sql = "SELECT * FROM AUTOGROUP";
        try (
            Statement statement = connection.createStatement();
        ) {

            ResultSet resultSet = statement.executeQuery(sql);
            System.out.println("\nПечать таблицы AUTOGROUP");
            while (resultSet.next()) {
                System.out.println("ID: " + resultSet.getInt("ID")
                        + "   BRAND: " + resultSet.getString("BRAND"));
            }
            if (resultSet.getRow()<0) {
                System.out.println("Таблица AUTOGROUP пуста");
            }
            resultSet.close();

        }
    }

    public void viewItems() throws SQLException {
        boolean haveresult;
        String sql = "SELECT * FROM AUTO";
        try (
            Statement statement = connection.createStatement();){

            haveresult = statement.execute(sql);
            if(haveresult){
                ResultSet resultSet = statement.getResultSet();
                System.out.println("\nПечать таблицы AUTO");
                while (resultSet.next()) {
                    System.out.println("ID: " + resultSet.getInt("ID")
                            + "   MODEL: " + resultSet.getString("MODEL")
                    + "   AUTOID: " + resultSet.getInt("AUTOID"));
                }
            }
            else{
                System.out.println("Таблица AUTO пуста");
            }
        }
    }

    public int getGroupID(String key) throws SQLException {
        int ID = -1;
        boolean have = false;
        String sql = "SELECT ID FROM AUTOGROUP WHERE BRAND = ?";
        try (
            PreparedStatement ppstmt = connection.prepareStatement(sql);
        ) {
            ppstmt.setString(1, key);

            ResultSet resultSet = ppstmt.executeQuery();
            while (resultSet.next()) {
                ID = resultSet.getInt("ID");
                have = true;
            }
            resultSet.close();
        }
        if (!have) System.out.println("Группа " + key + " не найдена");
        return ID;
    }

    public void viewItemsInGroup(int groupid) throws SQLException {
        String sqlRequest = "SELECT * FROM AUTO JOIN AUTOGROUP ON AUTO.AUTOID = ? and AUTOGROUP.ID = ?";

        ResultSet resultSet = null;
        try (
                PreparedStatement  preparedStatement = connection.prepareStatement(sqlRequest);
                )  {
            preparedStatement.setInt(1, groupid);
            preparedStatement.setInt(2, groupid);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("ID: " + resultSet.getInt("ID")
                        + " MODEL: " + resultSet.getString("BRAND")
                        + " AUTOID: " + resultSet.getInt("AUTOID"));
            }
        }

    }

    public void viewItemsInGroup(String groupName) throws SQLException {
        int ID;
        ID = getGroupID(groupName);
        if ( ID != -1){
            viewItemsInGroup(ID);
        }
    }

    public void createTableIfNeeded(){
        String sqlAuto = "SELECT * FROM AUTO";
        String sqlAutoGroup = "SELECT * FROM AUTOGROUP";
        boolean createdAutoGroup = false;
        boolean createdAuto = false;
        Statement statement = null;
        try{
            statement = connection.createStatement();
            //statement.execute("DROP TABLE AUTOGROUP IF EXISTS");
            //statement.execute("DROP TABLE AUTO IF EXISTS");
        }
        catch (SQLException e){
            e.printStackTrace();
        }

        try{
            if(statement.execute(sqlAutoGroup)){
                System.out.println("Таблица AUTOGROUP уже создана");
                createdAutoGroup = true;
            }
        }
        catch (SQLException e){
            System.out.println("Создание таблицы AUTOGROUP");
            String sqlCreateAutoGroup = "CREATE TABLE AUTOGROUP " +
                    "(ID INTEGER PRIMARY KEY generated always as identity," +
                    "BRAND VARCHAR(100)  UNIQUE NOT NULL);";
            try {
                statement.execute(sqlCreateAutoGroup);
            }
            catch (SQLException p){
                p.printStackTrace();
            }
        }

        try{
            if(statement.execute(sqlAuto)){
                System.out.println("Таблица AUTO уже создана");
                createdAuto = true;
            }
        }
        catch (SQLException e){
            System.out.println("Создание таблицы AUTO");
            String sqlCreateAuto = "CREATE TABLE AUTO " +
                    "(ID INTEGER PRIMARY KEY generated always as identity," +
                    "MODEL VARCHAR(100) UNIQUE NOT NULL, AUTOID INTEGER," +
                    "FOREIGN KEY (AUTOID) REFERENCES AUTOGROUP(ID) );";
            try {
                statement.execute(sqlCreateAuto);
            }
            catch (SQLException p){
                p.printStackTrace();
            }
        }

        if(!createdAutoGroup){
            System.out.println("Заполнение таблицы AUTOGROUP");
            try {
                statement.execute("INSERT INTO AUTOGROUP(BRAND) VALUES('Toyota');");
                statement.execute("INSERT INTO AUTOGROUP(BRAND) VALUES('Ford');");
                statement.execute("INSERT INTO AUTOGROUP(BRAND) VALUES('Nissan');");
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }

        if(!createdAuto){
            System.out.println("Заполнение таблицы AUTO");
            try {
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Avensis',0);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Prius',0);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Camry',0);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Land Cruiser',0);");

                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Focus',1);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Mondeo',1);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Fusion',1);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Fiesta',1);");

                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Almera',2);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Teana',2);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('GT-R',2);");
                statement.execute("INSERT INTO AUTO(MODEL,AUTOID) VALUES('Murano',2);");
            }
            catch (SQLException e){
                e.printStackTrace();
            }
        }
        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean addItemToGroup(String itemName, String groupName) {
        String sql = "INSERT INTO AUTO(MODEL,AUTOID) VALUES(?,?)";
        PreparedStatement ppstmt = null;
        try {
            if (getGroupID(groupName) != -1) {

                ppstmt = connection.prepareStatement(sql);
                ppstmt.setString(1, itemName);
                ppstmt.setInt(2, getGroupID(groupName));
                ppstmt.execute();
                return true;
            }
            else {
                return false;
            }
        } catch (SQLException e) {
        //e.printStackTrace();
        System.out.println("Такой AUTO уже имеется в базе");
        return false;
        }
    }

    public boolean removeItemFromGroup(String itemName, String groupName){
        String sql = "DELETE FROM AUTO WHERE MODEL = ? AND AUTOID = ?";
            try(PreparedStatement ppstmt = connection.prepareStatement(sql);){
                ppstmt.setString(1, itemName);
                ppstmt.setInt(2, getGroupID(groupName));
                if(ppstmt.executeUpdate()>0){
                    return true;
                }
                else {
                    System.out.println("Нет элемента с таким именем");
                    return false;
                }
            }
            catch (SQLException e){
                e.printStackTrace();
                System.out.println("Нет элемента с таким именем");
                return  false;
            }


    }

    public void itemReader(){
        try{
            connection.setAutoCommit(false);
            FileInputStream fstream = new FileInputStream("items.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            boolean toCommit = true;
            while ((strLine = br.readLine()) != null){
                if(itemReaderParser(strLine) == false){
                    toCommit = false;
                    break;
                }
            }
            if(toCommit){
                connection.commit();
                System.out.println("itemReader. Транзакция выполнена");
            }
            else {
                connection.rollback();
                System.out.println("itemReader. Откат транзакции");
            }
        }catch (IOException e){
            System.out.println("Ошибка ItemReader");
        }
        catch (SQLException s){
            System.out.println("Ошибка ItemReader");
        }
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public boolean itemReaderParser(String string){
        Pattern pattern = Pattern.compile("^([A-Za-z]*)" + "([+-])" + "([A-Za-z]*)$");
        Matcher matcher = pattern.matcher(string);
        if(matcher.find()){
            if(matcher.group(2).equals("+")){
                if(!(addItemToGroup(matcher.group(3), matcher.group(1)))){
                    return false;
                }
            }
            else {
                if(!(removeItemFromGroup(matcher.group(3), matcher.group(1)))){
                    return false;
                }
            }
        }
        return true;
    }

    public void groupReader(){
        try{
            connection.setAutoCommit(false);
            FileInputStream fstream = new FileInputStream("groups.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
            String strLine;
            while ((strLine = br.readLine()) != null){
                groupReaderParser(strLine);
            }
        }
        catch (IOException e){
            System.out.println("Ошибка groupReader");
        }
        catch (SQLException s){
            System.out.println("Ошибка groupReader");
        }
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        updateGroup();
    }

    public void groupReaderParser(String string){
        Pattern pattern = Pattern.compile("^([+-])"+"([A-Za-z]*)$");
        Matcher matcher = pattern.matcher(string);
        if(matcher.find()){
            if(matcher.group(1).equals("+")){
                addSet.add(matcher.group(2));
            }
            else {
                removeSet.add(matcher.group(2));
            }
        }
    }

    public void updateGroup(){
        Set<String> set = new HashSet<String>();
        boolean mayAdd = true;
        for (String groupNameAdd: addSet) {
            if(removeSet.contains(groupNameAdd)){
                removeSet.remove(groupNameAdd);
                set.add(groupNameAdd);
            }
        }
        for (String str: set) {
            if(addSet.contains(str)){
                addSet.remove(str);
            }
        }
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try (

            Statement statement = connection.createStatement
                    (ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = statement.executeQuery("SELECT * FROM AUTOGROUP");
                )
        {
            while (rs.next()){
                String check = rs.getString("BRAND");
                if(addSet.contains(check)){
                    connection.rollback();
                    System.out.println("rollback");
                    mayAdd = false;
                    break;
                }
                if(removeSet.contains(check)){
                    rs.deleteRow();
                    removeSet.remove(check);
                }
            }
            if(!removeSet.isEmpty()){
                System.out.println("rollback");
                connection.rollback();
            }
            else{
                if(mayAdd) {
                    for (String str : addSet) {
                        rs.moveToInsertRow();
                        rs.updateString("BRAND", str);
                        rs.updateNull("ID");
                        rs.insertRow();
                    }
                    connection.commit();
                }
            }

        }
        catch (SQLException e){
            e.printStackTrace();
        }
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}
