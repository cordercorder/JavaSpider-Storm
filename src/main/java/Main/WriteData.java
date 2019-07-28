package Main;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class WriteData {
    private final String JDBC_DRIVER="com.mysql.jdbc.Driver";

    private String user;

    private String password;

    private String db_url;

    private Connection connection;

    private Statement statement;

    public WriteData(String user,String password,String db_url){
        this.user=user;
        this.password=password;
        this.db_url=db_url;
        try {
            Class.forName(JDBC_DRIVER);
            connection=DriverManager.getConnection(db_url,user,password);
            statement=connection.createStatement();
            System.out.println("Connection succeed");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void InsertData(long id,String language,long count){
        String sql="insert into data(id,language,count) values("+id+",'"+language+"',"+count+")";
        try {
            statement.execute(sql);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
