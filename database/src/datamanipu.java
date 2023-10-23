import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class datamanipu {
    private Connection con = null;
    private ResultSet resultSet;

    private String host = "localhost";
    private String dbname = "postgres";
    private String user = "test";
    private String pwd = "Jianuo123?";
    private String port = "5432";


    public Connection getConnection() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return con;
    }


    public void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void slow_reader(Connection con){
        //run for about 20mins
        String csvFilePath = "E:\\OneDrive\\Code\\SQL\\database_project\\users.csv";
        try (
                BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        ) {
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            System.out.println("Headers: " + headerMap);
            int cnt = 0;
            // 读取CSV文件的内容并遍历每一行
            for (CSVRecord record : csvParser) {
                cnt++;
                System.out.println(cnt);
                //if(cnt == 2) break;
                /*String Mid = record.get("Mid");
                String Birthday = record.get("Birthday");
                String following = record.get("following");
                String identity = record.get("identity");
                String Level = record.get("Level");
                String Name = record.get("Name");
                String Sex = record.get("Sex");
                String Sign = record.get("Sign");
                // 更多列...

                // 创建插入数据库的SQL语句
                //String sql = "insert into test (l1) " +
                //        "values (?)";
                String sql = "insert into t_user (mid, name, sex, birthday, level, sign, identity) " +
                                "values (?,?,?,?,?,?,?)";
                PreparedStatement statement = con.prepareStatement(sql);
                statement.setBigDecimal(1, new BigDecimal(Mid));
                statement.setString(2, Name);
                statement.setString(3, Sex);
                statement.setString(4, Birthday);
                statement.setInt(5, Integer.parseInt(Level));
                statement.setString(6, Sign);
                statement.setString(7, identity);*/
                String Mid = record.get("Mid");
                String following = record.get("following");
                String followInfo[] = following.split(",");
                BigDecimal midInfo[] = new BigDecimal[100000];
                String sql = "insert into follows (followee, follower) " +
                        "values (?,?)";
                PreparedStatement statement = con.prepareStatement(sql);
                for(int i=0; i<followInfo.length; i++) {
                    if(followInfo.length == 1) break;
                    //System.out.printf("%s ", followInfo[i]);
                    String substr[] = followInfo[i].split("\'");
                    midInfo[i] = new BigDecimal(substr[1]);
                    //System.out.printf("%s\n", midInfo[i]);

                    statement.setBigDecimal(1, new BigDecimal(Mid));
                    statement.setBigDecimal(2, midInfo[i]);
                    statement.executeUpdate();
                    statement.close();
                }
                /*
                PreparedStatement statement = con.prepareStatement(sql);
                statement.setBigDecimal(1, new BigDecimal(Mid));
                statement.executeUpdate();
                statement.close();
                 */
            }
            System.out.println("Inserted records into the table...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void not_auto_commit_reader(Connection con) throws SQLException {
        con.setAutoCommit(false);
        String csvFilePath = "E:\\OneDrive\\Code\\SQL\\database_project\\users.csv";
        try (
                BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        ) {
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            System.out.println("Headers: " + headerMap);
            int cnt = 0;
            // 读取CSV文件的内容并遍历每一行
            for (CSVRecord record : csvParser) {
                cnt++;
                System.out.println(cnt);
                //if(cnt == 2) break;
                /*String Mid = record.get("Mid");
                String Birthday = record.get("Birthday");
                String following = record.get("following");
                String identity = record.get("identity");
                String Level = record.get("Level");
                String Name = record.get("Name");
                String Sex = record.get("Sex");
                String Sign = record.get("Sign");
                // 更多列...

                // 创建插入数据库的SQL语句
                //String sql = "insert into test (l1) " +
                //        "values (?)";
                String sql = "insert into t_user (mid, name, sex, birthday, level, sign, identity) " +
                                "values (?,?,?,?,?,?,?)";
                PreparedStatement statement = con.prepareStatement(sql);
                statement.setBigDecimal(1, new BigDecimal(Mid));
                statement.setString(2, Name);
                statement.setString(3, Sex);
                statement.setString(4, Birthday);
                statement.setInt(5, Integer.parseInt(Level));
                statement.setString(6, Sign);
                statement.setString(7, identity);*/
                String Mid = record.get("Mid");
                String following = record.get("following");
                String followInfo[] = following.split(",");
                BigDecimal midInfo[] = new BigDecimal[100000];
                String sql = "insert into follows (followee, follower) " +
                        "values (?,?)";
                PreparedStatement statement = con.prepareStatement(sql);
                for(int i=0; i<followInfo.length; i++) {
                    if(followInfo.length == 1) break;
                    //System.out.printf("%s ", followInfo[i]);
                    String substr[] = followInfo[i].split("\'");
                    midInfo[i] = new BigDecimal(substr[1]);
                    //System.out.printf("%s\n", midInfo[i]);

                    statement.setBigDecimal(1, new BigDecimal(Mid));
                    statement.setBigDecimal(2, midInfo[i]);
                    statement.executeUpdate();
                }
                statement.close();
                /*
                PreparedStatement statement = con.prepareStatement(sql);
                statement.setBigDecimal(1, new BigDecimal(Mid));
                statement.executeUpdate();
                statement.close();
                 */
            }
            con.commit();
            System.out.println("Inserted records into the table...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void good_reader(Connection con) throws SQLException {
        //takes about 2mins
        con.setAutoCommit(false);
        final int  BATCH_SIZE = 500;
        String csvFilePath = "E:\\OneDrive\\Code\\SQL\\database_project\\users.csv";
        try (
                BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
                BufferedReader reader2 = new BufferedReader(new FileReader(csvFilePath));
                CSVParser csvParser2 = new CSVParser(reader2, CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim());
        ) {
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            System.out.println("Headers: " + headerMap);
            int line_cnt = 0, command_cnt=0;
            // 读取CSV文件的内容并遍历每一行
            String sql1 = "insert into t_user (mid, name, sex, birthday, level, sign, identity) " +
                    "values (?,?,?,?,?,?,?)";
            PreparedStatement statement1 = con.prepareStatement(sql1);
            String sql = "insert into follows (followee, follower) " +
                    "values (?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            BigDecimal midInfo[] = new BigDecimal[100000];
            for (CSVRecord record : csvParser) {
                line_cnt++;
                System.out.println(line_cnt);
                //if(cnt == 2) break;
                String Mid = record.get("Mid");
                String Birthday = record.get("Birthday");
                String following = record.get("following");
                String identity = record.get("identity");
                String Level = record.get("Level");
                String Name = record.get("Name");
                String Sex = record.get("Sex");
                String Sign = record.get("Sign");
                statement1.setBigDecimal(1, new BigDecimal(Mid));
                statement1.setString(2, Name);
                statement1.setString(3, Sex);
                statement1.setString(4, Birthday);
                statement1.setInt(5, Integer.parseInt(Level));
                statement1.setString(6, Sign);
                statement1.setString(7, identity);
                statement1.addBatch();
                if(line_cnt % BATCH_SIZE == 0){
                    statement1.executeBatch();
                    statement1.clearBatch();
                }
            }
            statement1.executeBatch();
            statement1.clearBatch();
            statement1.close();
            con.commit();
            line_cnt = 0;
            for (CSVRecord record : csvParser2) {
                line_cnt++;
                System.out.println(line_cnt);
                //if(cnt == 2) break;
                String Mid = record.get("Mid");
                String following = record.get("following");
                String followInfo[] = following.split(",");
                for(int i=0; i<followInfo.length; i++) {
                    if(followInfo.length == 1) break;
                    //System.out.printf("%s ", followInfo[i]);
                    String substr[] = followInfo[i].split("\'");
                    midInfo[i] = new BigDecimal(substr[1]);
                    //System.out.printf("%s\n", midInfo[i]);
                    command_cnt++;
                    statement.setBigDecimal(1, new BigDecimal(Mid));
                    statement.setBigDecimal(2, midInfo[i]);
                    statement.addBatch();
                    if(command_cnt % BATCH_SIZE == 0){
                        statement.executeBatch();
                        statement.clearBatch();
                    }
                }
            }
            if(command_cnt % BATCH_SIZE != 0){
                statement.executeBatch();
                statement.clearBatch();
            }
            con.commit();
            statement.close();
            System.out.println("Inserted records into the table...");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
