package cn.trustfar.db;

import cn.trustfar.db.po.ViewData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBUtil {

    /**
     * 执行更新
     *
     * @param sql 传入的预设的sql语句
     * @param params 问号参数列表
     * @return 影响行数
     */
    public static synchronized int execUpdate(String sql, Object[] params) {
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = ConnectionPool.getInstance().getConnection();
            pstmt = con.prepareStatement(sql);

            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i] + "");
                }
            }

            return pstmt.executeUpdate();

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().close(null, pstmt, con);
        }
        return 0;
    }

    /**
     * 执行更新
     *
     * @param sql 传入的预设的sql语句
     * @param params 问号参数列表
     * @return 影响行数
     */
    public static synchronized int insert(String sql, ViewData params) {
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = ConnectionPool.getInstance().getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1,params.getSystemId());
            pstmt.setInt(2,params.getIndexId());
            pstmt.setString(3,params.getStatisticsTime());
            pstmt.setString(4,params.getStatisticalKey());
            pstmt.setString(5,params.getStatisticalValue());
            return pstmt.executeUpdate();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().close(null, pstmt, con);
        }
        return 0;
    }

    /**
     * 执行更新
     *
     * @param sql 传入的预设的sql语句
     * @param params 问号参数列表
     * @return 影响行数
     */
    public static synchronized int batch(String sql, ViewData params) {
        Connection con = null;
        PreparedStatement pstmt = null;
        try {
            con = ConnectionPool.getInstance().getConnection();
            pstmt = con.prepareStatement(sql);
            pstmt.setInt(1,params.getSystemId());
            pstmt.setInt(2,params.getIndexId());
            pstmt.setString(3,params.getStatisticsTime());
            pstmt.setString(4,params.getStatisticalKey());
            pstmt.setString(5,params.getStatisticalValue());
            return pstmt.executeUpdate();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            ConnectionPool.getInstance().close(null, pstmt, con);
        }
        return 0;
    }


}
