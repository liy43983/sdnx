package cn.trustfar.db;

import cn.trustfar.db.po.ViewData;
import cn.trustfar.utils.TimeUtil;
//import com.alibaba.druid.pool.DruidDataSource;
//import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionPool {

    private static DmcDataSource pool = null;

    private static ConnectionPool instance = null;

    private ConnectionPool(){}

    public static ConnectionPool getInstance(){
        if (instance == null) {
            instance = new ConnectionPool();
//            FileInputStream fis = null;
            Properties properties = new Properties();
            try {
//                fis = new FileInputStream("src/main/resources/DBConfig.properties");//本地
                properties.load(ConnectionPool.class.getResourceAsStream("/DBConfig.properties"));//打包
                pool = new DmcDataSource();
                pool.config(properties);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return instance;
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        ConnectionPool.getInstance();
        String dmcTimeStamp = TimeUtil.getDMCTimeStamp();
        DBUtil.insert(SQLUtil.INSERT_VIEW_DATA,new ViewData(2,1,dmcTimeStamp,"test","test"));

    }


    /**
     * 获得连接对象
     *
     * @return 连接对象
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public Connection getConnection() throws ClassNotFoundException,
            SQLException {
        return pool.getConnection();
    }

    /**
     * 关闭连接
     *
     * @throws SQLException
     */
    public synchronized void close(ResultSet rs, PreparedStatement pstmt, Connection con) {

        try {
            if (rs != null)
                rs.close();
            if (pstmt != null)
                pstmt.close();
            if (con != null)
                con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
