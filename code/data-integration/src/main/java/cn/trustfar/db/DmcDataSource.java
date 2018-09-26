package cn.trustfar.db;

import org.apache.commons.dbcp.BasicDataSource;

import java.io.ByteArrayInputStream;
import java.util.Enumeration;
import java.util.Properties;

//import com.alibaba.druid.pool.DruidDataSource;

public class DmcDataSource extends BasicDataSource {

    private String charset = "GBK";

    private String writeType = "2";

    public DmcDataSource() {
        super();
    }

    public String getCharset() {
        return charset;
    }


    public void setCharset(String charset) {
        this.charset = charset;
        super.addConnectionProperty("charset", this.charset);
    }

    public String getWriteType() {
        return writeType;
    }

    public void setWriteType(String writeType) {
        this.writeType = writeType;
        super.addConnectionProperty("write_type", this.writeType);
    }

    public void config(Properties properties) throws Exception{
        DmcDataSource dataSource = this;
        String value = null;
        value = properties.getProperty("defaultAutoCommit");
        if (value != null) {
            dataSource.setDefaultAutoCommit(Boolean.valueOf(value));
        }

        value = properties.getProperty("defaultReadOnly");
        if (value != null) {
            dataSource.setDefaultReadOnly(Boolean.valueOf(value));
        }

        value = properties.getProperty("defaultTransactionIsolation");
        if (value != null) {
            int level;
            if ("NONE".equalsIgnoreCase(value)) {
                level = 0;
            } else if ("READ_COMMITTED".equalsIgnoreCase(value)) {
                level = 2;
            } else if ("READ_UNCOMMITTED".equalsIgnoreCase(value)) {
                level = 1;
            } else if ("REPEATABLE_READ".equalsIgnoreCase(value)) {
                level = 4;
            } else if ("SERIALIZABLE".equalsIgnoreCase(value)) {
                level = 8;
            } else {
                try {
                    level = Integer.parseInt(value);
                } catch (NumberFormatException var6) {
                    System.err.println("Could not parse defaultTransactionIsolation: " + value);
                    System.err.println("WARNING: defaultTransactionIsolation not set");
                    System.err.println("using default value of database driver");
                    level = -1;
                }
            }

            dataSource.setDefaultTransactionIsolation(level);
        }

        value = properties.getProperty("defaultCatalog");
        if (value != null) {
            dataSource.setDefaultCatalog(value);
        }

        value = properties.getProperty("driverClassName");
        if (value != null) {
            dataSource.setDriverClassName(value);
        }

        value = properties.getProperty("maxActive");
        if (value != null) {
            dataSource.setMaxActive(Integer.parseInt(value));
        }

        value = properties.getProperty("maxIdle");
        if (value != null) {
            dataSource.setMaxIdle(Integer.parseInt(value));
        }

        value = properties.getProperty("minIdle");
        if (value != null) {
            dataSource.setMinIdle(Integer.parseInt(value));
        }

        value = properties.getProperty("initialSize");
        if (value != null) {
            dataSource.setInitialSize(Integer.parseInt(value));
        }

        value = properties.getProperty("maxWait");
        if (value != null) {
            dataSource.setMaxWait(Long.parseLong(value));
        }

        value = properties.getProperty("testOnBorrow");
        if (value != null) {
            dataSource.setTestOnBorrow(Boolean.valueOf(value));
        }

        value = properties.getProperty("testOnReturn");
        if (value != null) {
            dataSource.setTestOnReturn(Boolean.valueOf(value));
        }

        value = properties.getProperty("timeBetweenEvictionRunsMillis");
        if (value != null) {
            dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(value));
        }

        value = properties.getProperty("numTestsPerEvictionRun");
        if (value != null) {
            dataSource.setNumTestsPerEvictionRun(Integer.parseInt(value));
        }

        value = properties.getProperty("minEvictableIdleTimeMillis");
        if (value != null) {
            dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(value));
        }

        value = properties.getProperty("testWhileIdle");
        if (value != null) {
            dataSource.setTestWhileIdle(Boolean.valueOf(value));
        }

        value = properties.getProperty("password");
        if (value != null) {
            dataSource.setPassword(value);
        }

        value = properties.getProperty("url");
        if (value != null) {
            dataSource.setUrl(value);
        }

        value = properties.getProperty("username");
        if (value != null) {
            dataSource.setUsername(value);
        }

        value = properties.getProperty("validationQuery");
        if (value != null) {
            dataSource.setValidationQuery(value);
        }

        value = properties.getProperty("accessToUnderlyingConnectionAllowed");
        if (value != null) {
            dataSource.setAccessToUnderlyingConnectionAllowed(Boolean.valueOf(value));
        }

        value = properties.getProperty("removeAbandoned");
        if (value != null) {
            dataSource.setRemoveAbandoned(Boolean.valueOf(value));
        }

        value = properties.getProperty("removeAbandonedTimeout");
        if (value != null) {
            dataSource.setRemoveAbandonedTimeout(Integer.parseInt(value));
        }

        value = properties.getProperty("logAbandoned");
        if (value != null) {
            dataSource.setLogAbandoned(Boolean.valueOf(value));
        }

        value = properties.getProperty("poolPreparedStatements");
        if (value != null) {
            dataSource.setPoolPreparedStatements(Boolean.valueOf(value));
        }

        value = properties.getProperty("maxOpenPreparedStatements");
        if (value != null) {
            dataSource.setMaxOpenPreparedStatements(Integer.parseInt(value));
        }

        value = properties.getProperty("connectionProperties");
        if (value != null) {
            Properties p = getProperties(value);
            Enumeration e = p.propertyNames();

            while(e.hasMoreElements()) {
                String propertyName = (String)e.nextElement();
                dataSource.addConnectionProperty(propertyName, p.getProperty(propertyName));
            }
        }

        value = properties.getProperty("charset");
        if (value != null) {
            dataSource.setCharset(value);
        }

        value = properties.getProperty("writeType");
        if (value != null) {
            dataSource.setWriteType(value);
        }
    }
    private static Properties getProperties(String propText) throws Exception {
        Properties p = new Properties();
        if (propText != null) {
            p.load(new ByteArrayInputStream(propText.replace(';', '\n').getBytes()));
        }

        return p;
    }

}
