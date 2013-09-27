/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.sql.SQLException;

/**
 *
 * @author udomo
 */
public class DBException extends RuntimeException
{
    private String sql = "";
    private String msg = "";
    public DBException(Exception ex, String sql)
    {
        if(ex instanceof ClassNotFoundException)
        {
            msg = "Mysql driver is not found.";
        }
        else if (ex instanceof SQLException)
        {
            msg = ex.getMessage();
        }
        else
        {
            msg = ex.getMessage();
        }
        this.sql = sql;
    }
    public DBException(String msg, String sql)
    {
        super(msg);
        this.sql = sql;
    }
    @Override
    public String getMessage()
    {
        return msg+" \nFor SQL: "+sql;
    }
}
