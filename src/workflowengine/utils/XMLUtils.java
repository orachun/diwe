/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author udomo
 */
public class XMLUtils
{

    private static Transformer tx = null;
    private static boolean isInited = false;

    public static void init()
    {
        if (isInited)
        {
            return;
        }
        try
        {
            tx = TransformerFactory.newInstance().newTransformer();
            tx.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            tx.setOutputProperty(OutputKeys.INDENT, "no");
            isInited = true;
        }
        catch (TransformerFactoryConfigurationError | TransformerConfigurationException | IllegalArgumentException e)
        {
            e.printStackTrace();
        }
    }

    public static String nodeToString(Node n)
    {
        init();
        try
        {
            DOMSource src = new DOMSource(n);
            StringWriter sr = new StringWriter();
            Result res = new StreamResult(sr);
            tx.transform(src, res);
            return sr.toString();
        }
        catch (Exception e)
        {
            return (e.getMessage());
        }
    }
    
    public static Element strToNode(String xmlStr)
    {
        try
        {
            return (Element)DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder()
                    .parse(new InputSource( new StringReader( xmlStr ) ))
                    .getDocumentElement();
        }
        catch (ParserConfigurationException | SAXException | IOException e)
        {
            return null;
        }
    }
    
    public static String argumentTagToCmd(Element jobElement)
    {
//        NodeList nl = jobElement.getChildNodes();
//        for(int i=0;i<nl.getLength();i++)
//        {
//            System.out.println(nl.item(i).getTextContent());
//        }
//        System.exit(1);
        
        
//        Node n = jobElement.getElementsByTagName("argument").item(0);
        String taskName = jobElement.getAttribute("name");
//        String nodeString = nodeToString(n);
//        nodeString = nodeString.replace("<"+n.getNodeName()+">", "");
//        nodeString = nodeString.replace("</"+n.getNodeName()+">", "");
//        nodeString = nodeString.trim();
//        String[] lines = nodeString.split("\n");
        StringBuilder cmd = new StringBuilder(taskName).append(";");
        
        
        
        NodeList argList = jobElement.getElementsByTagName("argument").item(0).getChildNodes();
        for(int i=0;i<argList.getLength();i++)
        {
            Node c = argList.item(i);
            String cStr;
            if(c.getNodeName().equals("file"))
            {
                Element ec = (Element)c;
                cStr = (ec.getAttribute("name")).trim();
                if(!cStr.isEmpty())
                {
                    cmd.append(cStr).append(";");
                }
            }
            else
            {
                cStr = (c.getTextContent().trim());
                String[] cStrs = cStr.split("\\s+");
                for(String cs : cStrs)
                {
                    cs = cs.trim();
                    if(!cs.isEmpty())
                    {
                        cmd.append(cs).append(";");
                    }
                }
            }
            
            
        }
        
//        for(int i=0;i<lines.length;i++)
//        {
//            lines[i] = lines[i].trim();
//            Element e = strToNode(lines[i]);
//            if(e != null)
//            {
//                lines[i] = e.getAttribute("name");
//            }
//            lines[i] = lines[i].replace("<file name=\"", "");
//            lines[i] = lines[i].replace("\"/>", "");
//            lines[i] = lines[i].trim();
//            cmd.append(lines[i]).append(";");
//            
//        }
        cmd.replace(cmd.length()-1, cmd.length(), "");
//        System.out.println(cmd.toString());
        return cmd.toString();
    }
}
