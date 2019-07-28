package Main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class TestMysql {
    static final String JDBC_DRIVER="com.mysql.jdbc.Driver";

    static final String DB_URL="jdbc:mysql://localhost:3306/storm";

    static final String user="root";

    static final String password="123456";

    public static void main(String[] args){
        Connection connection;
        Statement statement;
        try{
            Class.forName(JDBC_DRIVER);
            System.out.println("连接数据库......");
            connection= DriverManager.getConnection(DB_URL,user,password);
            statement=connection.createStatement();
            String sql="select * from data";
            ResultSet res=statement.executeQuery(sql);
            while(res.next()){
                String language=res.getString("language");
                long id=res.getLong("id");
                long count=res.getLong("count");
                System.out.println("id------>"+id+"language=="+language+"------>count=="+count);

            }
            sql="insert into data(id,language,count) values(1000000000002,'c++13',30)";
            statement.execute(sql);
            sql="insert into data(id,language,count) values(1000000000003,'c++13',30)";
            statement.execute(sql);
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

}
