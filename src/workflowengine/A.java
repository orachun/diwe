/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.awt.Dimension;
import java.io.IOException;
import javax.swing.JEditorPane;
import javax.swing.JFrame;

/**
 *
 * @author orachun
 */
public class A extends JFrame
{
	public A()throws IOException
	{
		JEditorPane editor = new JEditorPane("http://www.google.com/");
		//editor.setEditable(false);
		editor.setPreferredSize(new Dimension(1024, 768));
		
		this.add(editor);
		this.setDefaultCloseOperation(EXIT_ON_CLOSE);
		this.setPreferredSize(new Dimension(1024, 768));
		this.setVisible(true);
	}
    public static void main(String[] args) throws Exception
    {
        new A();
    }
}
