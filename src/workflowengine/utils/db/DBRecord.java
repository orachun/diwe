/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

/**
 *
 * @author udomo
 */
public class DBRecord
{
    public static final Object DB_LOCKER = new Object();
    private HashMap<String, String> record = new HashMap<>();
    private String table;
    private static Connection con = null;

    public DBRecord()
    {
    }

	public DBRecord(String table)
	{
		this.table = table;
	}
	
	@Deprecated
    public DBRecord(String table, Object... vals)
    {
        if (vals.length % 2 == 1)
        {
            throw new IllegalArgumentException("The number of vals must be even.");
        }
        this.table = table;
        for (int i = 0; i < vals.length; i += 2)
        {
            record.put(vals[i].toString(), vals[i + 1].toString());
        }
    }

    public static void prepareConnection()
    {
        synchronized (DB_LOCKER)
        {
            if (con == null)
            {
                try
                {
                    String url = "jdbc:mysql://" + Utils.getProp("DBHost") + ":" + Utils.getProp("DBPort") + "/";
//                    System.out.println(url);
                    Class.forName("com.mysql.jdbc.Driver");
                    con = DriverManager.getConnection(
                            url,
                            Utils.getProp("DBUser"),
                            Utils.getProp("DBPass"));
					initDB();
					url += Utils.getProp("DBName");
                    con = DriverManager.getConnection(
                            url,
                            Utils.getProp("DBUser"),
                            Utils.getProp("DBPass"));
					
                }
                catch (ClassNotFoundException | SQLException ex)
                {
                    throw new DBException(ex, "");
                }
            }
        }
    }

    public DBRecord set(String key, String val)
    {
        record.put(key, val);
        return this;
    }

    public DBRecord set(String key, double val)
    {
        set(key, val + "");
        return this;
    }

    public String get(String key)
    {
        return record.get(key);
    }

    public double getDouble(String key)
    {
        return Double.parseDouble(record.get(key));
    }
	
	public long getLong(String key)
	{
		return Long.parseLong(record.get(key));
	}

    public int getInt(String key)
    {
        return Integer.parseInt(record.get(key));
    }

    public void unset(String key)
    {
        record.remove(key);
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    public int getFieldCount()
    {
        return record.size();
    }

    /**
     * Insert this record if not exist in DB. Otherwise, return the value of
     * first primary key of the first found record.
     *
     * @return
     */
    public int insertIfNotExist()
    {
        synchronized (DB_LOCKER)
        {
            List<DBRecord> res = select(table, this);
            if (res.isEmpty())
            {
                return insert();
            }
            else
            {
                return res.get(0).getInt(getFirstPrimaryKeyName(table));
            }
        }
    }
	
	public int upsert(String[] keys)
	{
		synchronized (DB_LOCKER)
        {
			DBRecord where = new DBRecord();
			for(String k : keys)
			{
				where.set(k, this.get(k));
			}
			List<DBRecord> res = select(table, where);
            if (res.isEmpty())
            {
                return insert();
            }
			else
			{
				return this.update(where);
			}
		}
	}

    public String getFirstPrimaryKeyName(String table)
    {
        synchronized (DB_LOCKER)
        {
            return select("SHOW KEYS FROM " + table + " WHERE Key_name = 'PRIMARY'").get(0).get("Column_name");
        }
    }

    public int insert()
    {
        synchronized (DB_LOCKER)
        {
            StringBuilder query = new StringBuilder();
            try
            {
                if (!Utils.isDBEnabled())
                {
                    return -2;
                }
                prepareConnection();
                query.append("INSERT INTO ");
                query.append(table);
                query.append(" ( ");
                for (String key : record.keySet())
                {
                    query.append(" ").append(key).append(", ");
                }
                query.delete(query.length() - 2, query.length());
                query.append(") VALUES ( ");
                for (String key : record.keySet())
                {
                    query.append(" '").append(record.get(key)).append("', ");
                }
                query.delete(query.length() - 2, query.length());
                query.append(" ) ");
//            System.out.println(query);
                Statement smt = con.createStatement();
                smt.executeUpdate(query.toString(), Statement.RETURN_GENERATED_KEYS);
                try
				{
					ResultSet rs = smt.getGeneratedKeys();
					rs.next();
					return rs.getInt(1);
				}
				catch (SQLException e)
				{
					return 0;
				}
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, query.toString());
            }
        }
    }

    public int delete()
    {
        synchronized (DB_LOCKER)
        {
            StringBuilder query = new StringBuilder();
            try
            {
                if (!Utils.isDBEnabled())
                {
                    return -2;
                }
                prepareConnection();
                query.append("DELETE FROM ").append(table).append(" WHERE ");
                for (String key : record.keySet())
                {
                    query.append(" ").append(key).append("='").append(record.get(key)).append("', ");
                }
                query.delete(query.length() - 2, query.length());
                return con.createStatement().executeUpdate(query.toString());
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, query.toString());
            }
        }
    }

    public int update(DBRecord where)
    {
        synchronized (DB_LOCKER)
        {
            StringBuilder query = new StringBuilder();
            try
            {
                if (!Utils.isDBEnabled())
                {
                    return -2;
                }
                prepareConnection();
                query.append("UPDATE ").append(table).append(" SET ");
                for (String key : record.keySet())
                {
                    query.append(" ").append(key).append("='").append(record.get(key)).append("', ");
                }
                query.delete(query.length() - 2, query.length());

                if (where.getFieldCount() > 0)
                {
                    query.append(" WHERE 1 ");
                    for (String key : where.record.keySet())
                    {
                        query.append(" AND ").append(key).append("='").append(where.get(key)).append("'");
                    }
                }
//            System.out.println(query);
                return con.createStatement().executeUpdate(query.toString());
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, query.toString());
            }
        }
    }

    public int update(String[] whereKeys)
    {
        DBRecord where = new DBRecord();
        for (String s : whereKeys)
        {
            where.set(s, this.get(s));
        }
        return update(where);
    }

    public int update()
    {
        return update(new String[0]);
    }

    public static int update(String sql)
    {
        synchronized (DB_LOCKER)
        {
            try
            {
                if (!Utils.isDBEnabled())
                {
                    return -2;
                }
                prepareConnection();
                return con.createStatement().executeUpdate(sql);
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, sql);
            }
        }
    }

    public static List<DBRecord> selectAll(String table)
    {
        return select(table, new DBRecord());
    }

    public static List<DBRecord> select(String sql)
    {
        return select("", sql);
    }

    public static List<DBRecord> select(String table, DBRecord where)
    {
        if (!Utils.isDBEnabled())
        {
            return null;
        }
        prepareConnection();
        StringBuilder query = new StringBuilder();
        query.append("SELECT * FROM ").append(table);
        query.append(" WHERE 1 ");
        for (String key : where.record.keySet())
        {
            query.append(" AND ").append(key).append("='").append(where.get(key)).append("'");
        }
        return select(table, query.toString());
    }

	public static void exec(String sql)
	{
		synchronized (DB_LOCKER)
        {
            if (!Utils.isDBEnabled())
            {
                return ;
            }
            prepareConnection();
            try
            {
                con.createStatement().execute(sql);
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, sql);
            }
        }
	}
	
    public static List<DBRecord> select(String table, String sql)
    {
        synchronized (DB_LOCKER)
        {
            if (!Utils.isDBEnabled())
            {
                return null;
            }
            prepareConnection();
            try
            {
                ResultSet rs = con.createStatement().executeQuery(sql);
                ResultSetMetaData md = rs.getMetaData();
                ArrayList<DBRecord> results = new ArrayList<>();
                while (rs.next())
                {
                    DBRecord r = new DBRecord();
                    for (int i = 1; i <= md.getColumnCount(); i++)
                    {
                        r.set(md.getColumnLabel(i), rs.getString(i));
                    }
                    r.setTable(table);
                    results.add(r);
                }
                return results;
            }
            catch (SQLException ex)
            {
                throw new DBException(ex, sql);
            }
        }
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (String s : record.keySet())
        {
            sb.append(s).append(":").append(record.get(s)).append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        return sb.toString();
    }

	
	private static void initDB()
	{
		
		String dbname = Utils.getProp("DBName") + "_"
				+ Utils.getProp("local_hostname") + "_"
				+ Utils.getIntProp("local_port");
		Utils.getPROP().setProperty("DBName", dbname);
		System.out.println("Preparing db : "+dbname);
		DBRecord.update("DROP DATABASE IF EXISTS `"+dbname+"`");
		DBRecord.update("CREATE DATABASE IF NOT EXISTS `"+dbname+"`");
		
		String url = "jdbc:mysql://" 
				+ Utils.getProp("DBHost") + ":" 
				+ Utils.getProp("DBPort") + "/" 
				+ Utils.getProp("DBName");
		try
		{
			con = DriverManager.getConnection(
					url,
					Utils.getProp("DBUser"),
					Utils.getProp("DBPass"));
		}
		catch (SQLException ex)
		{
			throw new DBException(ex, "");
		}
		
		String sql = 
			"CREATE TABLE IF NOT EXISTS `exec_site` (\n" +
			"  `uri` varchar(40) NOT NULL,\n" +
			"  `total_processors` int(10) NOT NULL,\n" +
			"  PRIMARY KEY (`uri`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `exec_site_checkpoint` (\n" +
			"  `escid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `esid` int(11) NOT NULL,\n" +
			"  `tid` int(11) NOT NULL,\n" +
			"  `path` varchar(255) COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `checkpointed_at` int(11) NOT NULL,\n" +
			"  PRIMARY KEY (`escid`)\n" +
			") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci AUTO_INCREMENT=1 ;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `exec_site_file` (\n" +
			"  `es_uri` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `fid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  PRIMARY KEY (`es_uri`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `file` (\n" +
			"  `fid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `estsize` double NOT NULL DEFAULT '0',\n" +
			"  `priority` double NOT NULL DEFAULT '0',\n" +
			"  `file_type` char(1) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `real_size` bigint(20) NOT NULL DEFAULT '-1',\n" +
			"  PRIMARY KEY (`fid`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `schedule` (\n" +
			"  `sid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `tid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `wkid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `estimated_start` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `estimated_finish` int(11) NOT NULL DEFAULT '-1',\n" +
			"  PRIMARY KEY (`sid`)\n" +
			") ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=38 ;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `task` (\n" +
			"  `tid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',\n" +
			"  `cmd` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `status` char(1) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL COMMENT '''W''aiting, ''E''xecuting, ''C''ompleted, ''S''uspended, ''F''ail',\n" +
			"  `estopr` int(11) NOT NULL COMMENT 'Estimated number of atomic operations',\n" +
			"  `start` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `finish` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `exit_value` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `priority` double NOT NULL DEFAULT '-1',\n" +
			"  PRIMARY KEY (`tid`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `task_exec_time` (\n" +
			"  `tetid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `wfname` varchar(255) COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `tname` varchar(255) COLLATE utf8_unicode_ci DEFAULT '',\n" +
			"  `exec_time` int(11) NOT NULL DEFAULT '-1',\n" +
			"  PRIMARY KEY (`tetid`),\n" +
			"  KEY `wfname` (`wfname`)\n" +
			") ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci AUTO_INCREMENT=1 ;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `worker` (\n" +
			"  `wkid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `esid` int(11) DEFAULT '0',\n" +
			"  `hostname` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `port` int(6) NOT NULL DEFAULT '-1',\n" +
			"  `cpu` float DEFAULT '0',\n" +
			"  `total_memory` float DEFAULT '0',\n" +
			"  `free_memory` float DEFAULT '0',\n" +
			"  `total_space` float DEFAULT '0',\n" +
			"  `free_space` float DEFAULT '0',\n" +
			"  `updated` int(11) DEFAULT '0',\n" +
			"  `unit_cost` int(11) DEFAULT '0',\n" +
			"  `current_tid` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `uuid` char(36) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',\n" +
			"  `esp_hostname` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',\n" +
			"  `esp_port` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `total_usage` int(11) NOT NULL DEFAULT '0',\n" +
			"  PRIMARY KEY (`wkid`),\n" +
			"  UNIQUE KEY `uuid` (`uuid`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 ;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `workflow` (\n" +
			"  `wfid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `superwfid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL DEFAULT '',\n" +
			"  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `submitted` int(11) NOT NULL,\n" +
			"  `status` char(1) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `started_at` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `scheduled_at` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `finished_at` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `est_finish` int(11) NOT NULL DEFAULT '-1',\n" +
			"  `cumulated_time` int(11) NOT NULL DEFAULT '0',\n" +
			"  PRIMARY KEY (`wfid`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `workflow_task` (\n" +
			"  `tid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `wfid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  PRIMARY KEY (`tid`,`wfid`)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `workflow_task_depen` (\n" +
			"  `wtdid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `parent` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `child` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `wfid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  PRIMARY KEY (`wtdid`)\n" +
			") ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=7 ;\n" +
			"\n";
		DBRecord.update(sql);
		sql = 
			"CREATE TABLE IF NOT EXISTS `workflow_task_file` (\n" +
			"  `wtfid` int(11) NOT NULL AUTO_INCREMENT,\n" +
			"  `type` char(1) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `tid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  `fid` varchar(40) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,\n" +
			"  PRIMARY KEY (`wtfid`)\n" +
			") ENGINE=InnoDB  DEFAULT CHARSET=utf8 AUTO_INCREMENT=481 ;";
		DBRecord.update(sql);
//		DBRecord.update("TRUNCATE `exec_site`;\n");
//		DBRecord.update("TRUNCATE `exec_site_checkpoint`;\n");
//		DBRecord.update("TRUNCATE `exec_site_file`;\n");
//		DBRecord.update("TRUNCATE `file`;\n");
//		DBRecord.update("TRUNCATE `schedule`;\n");
//		DBRecord.update("TRUNCATE `task`;\n");
//		DBRecord.update("TRUNCATE `task_exec_time`;\n");
//		DBRecord.update("TRUNCATE `worker`;\n");
//		DBRecord.update("TRUNCATE `workflow`;\n");
//		DBRecord.update("TRUNCATE `workflow_task`;\n");
//		DBRecord.update("TRUNCATE `workflow_task_depen`;\n" );
//		DBRecord.update("TRUNCATE `workflow_task_file`;");
	}
	
    public static void main(String[] args)
    {
    }
}
