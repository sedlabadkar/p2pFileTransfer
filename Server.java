/**
 * @author Sachin Edlabadkar
 *  TODO 4. Implement basic functions for client request
 *  TODO 5. Better Exception/ Failure handling
 */

import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.File;
/**
 * @author Sachin Edlabadkar
 *  TODO 4. Implement basic functions for client request
 *  TODO 5. Better Exception/ Failure handling
 */

public class Server {
	public static AtomicInteger totalNumClientsConnected = new AtomicInteger();
	String fileName;
	int listeningPort;
	Document doc;
	
	private int getMaxNumClients() throws Exception {
		BufferedReader br = new BufferedReader (new FileReader("config.txt"));
		int count = 0;
		String line;
		while ((line = br.readLine()) != null )
		{
			count++;
		}
		return count - 1;
	}
	
	private int loadConfig() throws Exception {
		File xmlConf = new File("config.xml");
		if (!xmlConf.exists())
			return -1;
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		doc = dBuilder.parse(xmlConf);
		doc.getDocumentElement().normalize();
		return 1;
	}
	
	private Element getConfigForID(int item) throws Exception{
		
		NodeList x = doc.getElementsByTagName("node");
		return (Element) x.item(item);
	}
	
	/**
	 * SplitFile
	 * Splits a file into small chunks of size 100KB
	 * Input: File Object to be split
	 * Output: Number of chunks formed
	 * Error: -1 returned
	 */
	private static int splitFile(File f)
	{
		int counter = 1, fileSize = 0, totalBytesRead = 0, bytesRead = 0;
		int chunkSize = 100 * 1024; //100KB
		String filename = f.getName();
		byte[] readBuffer;
		File outFile = new File("data/");
		if (!outFile.exists())
		{
			outFile.mkdir();
		}
		try
		{	
			if (f.exists())
			{
				FileInputStream fileIn = new FileInputStream(f);
				BufferedInputStream inputStream = new BufferedInputStream (fileIn);
				fileSize = (int)f.length();
				System.out.print("Filename = " + filename
						+ "\nSize = " + f.length() + "\n");
				while (totalBytesRead < fileSize)
				{
					int bytesRemaining = fileSize - totalBytesRead;
					if (bytesRemaining < chunkSize)
					{
						chunkSize = bytesRemaining;
					}
					readBuffer = new byte[chunkSize];
					bytesRead = inputStream.read(readBuffer);
					if (bytesRead > 0)
					{
						totalBytesRead += bytesRead;
						File chunk = new File ("data/" + filename + ".part" + counter++);
						FileOutputStream fileOut = new FileOutputStream(chunk);
						fileOut.write(readBuffer);
					}
				}
				return (counter - 1);
			}
			else 
				System.out.println ("File not found");
		}
		catch (Exception e)
		{
			System.out.println(e.toString());
		}
		return -1;
	}
	
	private void startServer() throws Exception{

		//Control Thread
		int fileNumChunks,clientNum = 0, maxNumClients = 0;
		Element confElem;
		int configLoad = loadConfig();
		if (configLoad < 0)
		{
			System.out.println ("Config Not Found. Abort");
			System.exit(0);
		}
		confElem = getConfigForID(0);
		listeningPort = Integer.parseInt(confElem.getElementsByTagName("port").item(0).getTextContent());
		maxNumClients = Integer.parseInt(confElem.getElementsByTagName("maxNumClients").item(0).getTextContent());
		System.out.print("Enter the name of the file:");
	    BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
	    fileName = bufferRead.readLine();
		File inputFile = new File(fileName);
		
		if ((fileNumChunks = splitFile(inputFile)) != -1)
			System.out.println ("File Split done. Number of Chunks = " + fileNumChunks);
		else
			System.exit(0);
		
		ServerSocket srv = new ServerSocket(listeningPort);
		System.out.println("Server running on port " + listeningPort);
		
		try
		{
    		while(true) 
    		{
    			totalNumClientsConnected.getAndIncrement();
				clientNum++;
				new ClientHandler(srv.accept(), totalNumClientsConnected.intValue(), fileNumChunks, fileName, doc, maxNumClients, totalNumClientsConnected).start();
    		}
		} 
		catch (Exception e) 
		{
			System.out.println ("Exception " + e);
		} 
		finally 
		{
			if (srv.isClosed() != true)
			{
				System.out.println ("Closing Server Socket");
				srv.close();
			}
			else
			{
				System.out.println ("Server socket not open");
			}
		}
	}
	
	public Server() {
		try{
			startServer();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Server s = new Server();
	}
}

/**
 * Client Thread
 * The thread responsible for dealing with a client
 * Control thread passes the client over to this thread. 
 * This thread is responsible for client related processing till the end of time
 * Should update a centrally maintained list of clients and chunks -- or should this be done in the control thread?
 */
class ClientHandler extends Thread {
	//client Thread
	Socket connection;
	int clientNum, numChunks, maxNumClients;
	String inMessage, outMessage, filename;
	ObjectOutputStream out;
	ObjectInputStream in;
	int[] chunksToSend;
	Document doc;
	AtomicInteger totalNumClientsConnected;
	
	public ClientHandler (Socket clientSock, int Num, int numChunks, String fileName, Document doc, int maxNumClients, AtomicInteger totalNumClientsConnected)
	{
		this.connection = clientSock;
		clientNum = Num;
		this.numChunks = numChunks;
		filename = fileName;
		this.doc = doc;
		this.maxNumClients = maxNumClients;
		this.totalNumClientsConnected = totalNumClientsConnected;
	}
	
	private void sendChunk (int chunkNum) throws Exception
	{
		if (chunkNum > 0)
		{
			byte[] chunk;
			File chunkFile = new File("data/" + filename + ".part" + chunkNum);
			if (chunkFile.exists())
			{
				int len;
				FileInputStream chunkIn = new FileInputStream(chunkFile);
				BufferedInputStream inputStream = new BufferedInputStream (chunkIn);
				chunk = new byte[(int)chunkFile.length()];
				if ((len = inputStream.read(chunk)) > 0)
				{
					outMessage = new String("CHNK:" + chunkNum);
					out.writeObject(outMessage);
					out.writeObject(chunk);
					System.out.println ("Sent chunk " + chunkNum + " to client " + clientNum + ". Bytes Sent = " + len);
				}
			}
			else 
				System.out.println ("Chunk does not exist. Can't Send");
		}
	}

	private int getChunksToSend(int[] chunksToSend)
	{
		//System.out.println ("getChunksToSend " + numChunks);
		int i;
		for (i = 0; i < (numChunks/maxNumClients + 1); i++)
		{
			chunksToSend[i] = clientNum + (i * maxNumClients);
			if (chunksToSend[i] > numChunks)
				chunksToSend[i] = -1;
		}
		return chunksToSend.length;
	}
	
	private void uploadToClient() throws Exception
	{
		//System.out.println ("Upload To Client");
		int i = 0, len = 0;
		out.writeObject("FILE:" + filename);
		outMessage = new String("CNT:" + numChunks);
		out.writeObject(outMessage);
		chunksToSend = new int[(numChunks/maxNumClients) + 1];
		len = getChunksToSend(chunksToSend);
		if (len > 0)
		{
			for (i = 0; i < chunksToSend.length; i++)
			{
				sendChunk(chunksToSend[i]);
			}
		}
		else
			System.out.println ("Something Wrong. Bad Things will happen. Abort");
	}
	
	private String getPortsForClientFromConfig(int clientNum, boolean downloadPort){
		NodeList x = doc.getElementsByTagName("node");
		int index;
		if (downloadPort)
		{
			index = Integer.parseInt(((Element)x.item(clientNum)).getElementsByTagName("downloadNeighbor").item(0).getTextContent());
		}
		else
		{
			index = clientNum;
		}
		return ((Element)x.item(index)).getElementsByTagName("port").item(0).getTextContent();
	}
	
	public void run()
	{
		int dwldPort, upldPort;
		try 
		{
			out = new ObjectOutputStream(connection.getOutputStream());
			in = new ObjectInputStream (connection.getInputStream());
			while (true)
			{
				inMessage = (String)in.readObject();
				System.out.println();
				if (inMessage.equals("connect"))
				{
					System.out.println ("Client " + clientNum + " Joined ");
					out.writeObject("ID:" + clientNum);
					out.writeObject("dwldPort:" + getPortsForClientFromConfig(clientNum, true).trim());
					out.writeObject("upldPort:" + getPortsForClientFromConfig(clientNum, false).trim());
					uploadToClient();
				}
				out.writeObject("end");
				System.out.println ("Uploaded all chunks to client " + clientNum); 
				//Thread will wait, in case client has to do further communication
				//Thread will only exit when the client closes it?
			}
		}catch (SocketException sockE){
			totalNumClientsConnected.getAndDecrement();
			System.out.println ("Client " +  clientNum + " Disconnected.");
		}catch (EOFException eof)
		{
			totalNumClientsConnected.getAndDecrement();
			System.out.println ("Client " +  clientNum + " Disconnected.");
		}catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
