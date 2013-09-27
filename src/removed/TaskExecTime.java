/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import java.io.Serializable;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.xml.bind.annotation.XmlRootElement;

/**
 *
 * @author orachun
 */
@Entity
@Table(name = "task_exec_time")
@XmlRootElement
@NamedQueries(
{
	@NamedQuery(name = "TaskExecTime.findAll", query = "SELECT t FROM TaskExecTime t"),
	@NamedQuery(name = "TaskExecTime.findByTetid", query = "SELECT t FROM TaskExecTime t WHERE t.tetid = :tetid"),
	@NamedQuery(name = "TaskExecTime.findByWfname", query = "SELECT t FROM TaskExecTime t WHERE t.wfname = :wfname"),
	@NamedQuery(name = "TaskExecTime.findByTname", query = "SELECT t FROM TaskExecTime t WHERE t.tname = :tname"),
	@NamedQuery(name = "TaskExecTime.findByExecTime", query = "SELECT t FROM TaskExecTime t WHERE t.execTime = :execTime")
})
public class TaskExecTime implements Serializable
{
	private static final long serialVersionUID = 1L;
	@Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "tetid")
	private Integer tetid;
	@Basic(optional = false)
    @Column(name = "wfname")
	private String wfname;
	@Column(name = "tname")
	private String tname;
	@Basic(optional = false)
    @Column(name = "exec_time")
	private int execTime;

	public TaskExecTime()
	{
	}

	public TaskExecTime(Integer tetid)
	{
		this.tetid = tetid;
	}

	public TaskExecTime(Integer tetid, String wfname, int execTime)
	{
		this.tetid = tetid;
		this.wfname = wfname;
		this.execTime = execTime;
	}

	public Integer getTetid()
	{
		return tetid;
	}

	public void setTetid(Integer tetid)
	{
		this.tetid = tetid;
	}

	public String getWfname()
	{
		return wfname;
	}

	public void setWfname(String wfname)
	{
		this.wfname = wfname;
	}

	public String getTname()
	{
		return tname;
	}

	public void setTname(String tname)
	{
		this.tname = tname;
	}

	public int getExecTime()
	{
		return execTime;
	}

	public void setExecTime(int execTime)
	{
		this.execTime = execTime;
	}

	@Override
	public int hashCode()
	{
		int hash = 0;
		hash += (tetid != null ? tetid.hashCode() : 0);
		return hash;
	}

	@Override
	public boolean equals(Object object)
	{
		// TODO: Warning - this method won't work in the case the id fields are not set
		if (!(object instanceof TaskExecTime))
		{
			return false;
		}
		TaskExecTime other = (TaskExecTime) object;
		if ((this.tetid == null && other.tetid != null) || (this.tetid != null && !this.tetid.equals(other.tetid)))
		{
			return false;
		}
		return true;
	}

	@Override
	public String toString()
	{
		
		return "removed.TaskExecTime[ tetid=" + tetid + " ]";
	}
	
}
