/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.monitor;

/**
 *
 * @author orachun
 */
public class HTMLUtils
{
	public static String nl2br(String html)
	{
		return html.replace("\n", "<br/>");
	}
	
	public static String icon(String html)
	{
		return html
				.replace("[[W]]", "")
				.replace("[[E]]", "")
				.replace("[[F]]", "")
				.replace("[C]", "");
	}
}
