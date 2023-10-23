import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Main {
    public static void main(String[] args) throws SQLException {
        datamanipu ins = new datamanipu();
        Connection con = ins.getConnection();
        ins.good_reader(con);
        ins.closeConnection();
    }
}