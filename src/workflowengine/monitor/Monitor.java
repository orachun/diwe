/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.monitor;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Set;
import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import org.jfree.chart.ChartPanel;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.WorkflowExecutorInterface;

/**
 *
 * @author orachun
 */
public class Monitor extends JFrame
{
	private static String css = ""
			+ "<style>"
			+ ".task-mapping-entry, .task-queue-entry"
			+ "{"
			+ "		display: inline-block;"
			+ "		width: 250px;"
			+ "		color: gray;"
			+ "		font-face: monospace;"
			+ "}"
			+ "</style>";
	private WorkflowExecutorInterface worker;
	private JEditorPane taskMappingPane = new JEditorPane("text/html", "");
	private JEditorPane taskQueuePane = new JEditorPane("text/html", "");
	private JEditorPane statusPane = new JEditorPane("text/html", "");
	private JPanel eventChartPanel = new JPanel(new BorderLayout());
	private JTextField uriTextField = new JTextField();
	private JButton shutdownBtn;
	private JButton refreshBtn;
	private JPanel buttonPanel;
	public Monitor(String uri) throws NotBoundException, RemoteException
	{
		uriTextField.setText(uri);
		this.setLayout(new BorderLayout());
		
		JTabbedPane tabs = new JTabbedPane();
		tabs.add("Status", new JScrollPane(statusPane));
		tabs.add("Task Mapping", new JScrollPane(taskMappingPane));
		tabs.add("Task Queue", new JScrollPane(taskQueuePane));
		tabs.add("Events", new JScrollPane(eventChartPanel));
		
		this.add(tabs, BorderLayout.CENTER);
		buttonPanel = new JPanel(new GridLayout(20, 1));
		refreshBtn = new JButton("Refresh");
		refreshBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					refresh();
				}
				catch(Exception ex2)
				{
					popup("Cannot connect to server:" + ex2.getMessage());
				}
			}
		});
		shutdownBtn = new JButton("Shutdown");
		shutdownBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e)
			{
				try
				{
					worker.shutdown();
				}
				catch(Exception ex2)
				{
					popup("Cannot connect to server:" + ex2.getMessage());
				}
			}
		});
		buttonPanel.add(shutdownBtn);
		buttonPanel.add(refreshBtn);
		this.add(uriTextField, BorderLayout.NORTH);
		this.add(buttonPanel, BorderLayout.WEST);
		
		refresh();
		this.setVisible(true);
		this.setSize(640, 480);
		this.setLocation(50, 50);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		
		
//		new Thread()
//		{
//			@Override
//			public void run()
//			{
//				while (true)
//				{
//					try
//					{
//						Thread.sleep(3000);
//						refresh();
//					}
//					catch (Exception e)
//					{
//					}
//				}
//			}
//		}.start();
	}
	
	private ChangeServerActionListener serverAL = new ChangeServerActionListener();
	private synchronized void refresh() throws RuntimeException
	{
		this.setTitle(uriTextField.getText());
		worker = WorkflowExecutor.getRemoteExecutor(uriTextField.getText());
		statusPane.setText(worker.getStatusHTML());
		
		taskMappingPane.setText(css+worker.getTaskMappingHTML());
		taskQueuePane.setText(css+worker.getTaskQueueHTML());
		
		eventChartPanel.removeAll();
		EventLogger events = worker.getEventLog();
		ChartPanel cp = new ChartPanel(events.toGanttChart());
		
		cp.setDomainZoomable(true);
		cp.setMinimumDrawHeight(20*events.size()+100);
		cp.setMaximumDrawHeight(20*events.size()+100);
		cp.setPreferredSize(new Dimension(800, 25*events.size()+100));
		eventChartPanel.setPreferredSize(new Dimension(800, 25*events.size()+100));
		
		eventChartPanel.add(cp);
		
		String managerURI = worker.getManagerURI();
		
		buttonPanel.removeAll();
		buttonPanel.add(shutdownBtn);
		buttonPanel.add(refreshBtn);
		if(managerURI != null)
		{
			JButton b = new JButton(managerURI);
			b.addActionListener(serverAL);
			buttonPanel.add(b);
		}
		Set<String> workers = worker.getWorkerSet();
		if(workers != null)
		{
			for(String uri : workers)
			{
				JButton b = new JButton(uri);
				b.addActionListener(serverAL);
				buttonPanel.add(b);
			}
		}
		buttonPanel.updateUI();
	}
	
	private void popup(String msg)
	{
		JOptionPane.showMessageDialog(this, msg);
	}
	
	public static void main(String[] args) throws NotBoundException, RemoteException
	{
		new Monitor("10.217.168.205:9900");
	}
	
	class ChangeServerActionListener implements ActionListener
	{

		@Override
		public void actionPerformed(ActionEvent e)
		{
			uriTextField.setText(((JButton)e.getSource()).getText());
			try
			{
				refresh();
			}
			catch(RuntimeException ex2)
			{
				popup("Cannot connect to server:" + ex2.getMessage());
			}
		}
	}
}
