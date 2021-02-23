import java.sql.*;

public class TestClass2 {

    public static void main(String[] args) throws Exception {
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://bgdata00:10000/default";
        String user = "hive";
        String password = "";

        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);

        Statement stat = conn.createStatement();

        long l = System.currentTimeMillis();

        PreparedStatement pstat = conn.prepareStatement("insert into internal_table1 partition (ds = '20210101') values ('aaaa',null,?,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)");

        pstat.setString(1,"yyyy");




        boolean execute = pstat.execute();
        System.out.println(execute);
        pstat.close();

        System.out.println(System.currentTimeMillis() -l);

        stat.close();
        conn.close();
    }
}
