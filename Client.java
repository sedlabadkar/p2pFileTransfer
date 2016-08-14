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
import java.util.concurrent.CyclicBarrier;


public class Client {
	static String inMessage,outMessage;
	String filename;
	int totalChunks, recvdChunkNum, srvPort, selfID, numChunksRecd = 0, dwldPort, upldPort;
	CyclicBarrier barrier1;
	Socket clientSock;
	ObjectOutputStream outStream;
	ObjectInputStream inStream;
	
	Runnable barrier1Action = new Runnable() {
	    public void run() {
	        System.out.println("");
	    }
	};

	private String readChunkSummary() throws Exception {
		File chunkSummary = new File ("chunksummary.txt");
		String line;
		if (chunkSummary.exists()){
			BufferedReader br = new BufferedReader(new FileReader(chunkSummary));
			line = br.readLine();
			return line;
		}
		return null;
	}
	
	private List<Integer> getLocalChunkList() throws Exception {
		File chunkSummary = new File ("chunksummary.txt");
		String line;
		LinkedList<Integer> list = new LinkedList<Integer>();
		if (chunkSummary.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(chunkSummary));
			line = br.readLine();
			StringTokenizer strTok = new StringTokenizer(line.trim());
			String nextTok;
			while (strTok.hasMoreTokens())
			{
				nextTok = strTok.nextToken(" ");
				list.add(Integer.parseInt(nextTok));
			}
			return list;
		}
		return null;
	}
	
	private void updateChunkSummary(int recvdChunkNum) throws Exception {
		//System.out.println ("UpdateChunkSymmary " + recvdChunkNum);
		File chunkSummary = new File ("chunksummary.txt");
    	if(!chunkSummary.exists()){
     	   chunkSummary.createNewFile();
     	}
    	FileWriter fw = new FileWriter(chunkSummary,true);
    	BufferedWriter bw = new BufferedWriter(fw);
    	bw.write(" " + recvdChunkNum);
    	bw.close();
	}
	
	
	private void mergeFile() throws Exception{
		//System.out.println("Merge file " + filename);
		File outputFile = new File ("output/");
		File inputFile;
		int i;
        FileInputStream fis;
		byte[] readBuffer;
        int bytesRead = 0;
        if (!outputFile.exists())
        {
        	outputFile.mkdir();
        }
        outputFile = new File("output/"+filename);
        FileOutputStream fos = new FileOutputStream (outputFile, true);        
		for (i = 1; i <= totalChunks; i++)
		{
			inputFile = new File("data/" + filename + ".part" + i);
			if (inputFile.exists())
			{	
				readBuffer = new byte[(int)inputFile.length()];
				fis = new FileInputStream(inputFile);
				fis.read(readBuffer);
				fos.write(readBuffer);
				fos.flush();
				fis.close();
				fis = null;
			}
		}
		System.out.println("File Saved at output/" + filename + " File Size = " + outputFile.length());
	}
	
	private void recv() throws Exception
	{
		while (true)
		{
			Object x;
			x = inStream.readObject();
			if (x.getClass().equals(String.class))
			{
				String inStr = (String) x;
				//System.out.println ("RECEIVED: " + inStr);
				if (inStr.toLowerCase().contains("dwldport"))
				{
					int index = inStr.lastIndexOf(":");
					String portStr = inStr.substring(index + 1); 
					dwldPort = Integer.parseInt(portStr.trim()); 
				}
				if (inStr.toLowerCase().contains("upldport"))
				{
					int index = inStr.lastIndexOf(":");
					String portStr = inStr.substring(index + 1); 
					upldPort = Integer.parseInt(portStr.trim()); 
				}
				if (inStr.toLowerCase().contains("chnk"))
				{
					int index = inStr.lastIndexOf(":");
					String chunkNum = inStr.substring(index + 1); //Assuming that no space is there
					recvdChunkNum = Integer.parseInt(chunkNum.trim()); //TODO incorrect value handling
				}
				else if (inStr.toLowerCase().contains("end"))
				{
					System.out.println ("Main Thread: Download from server finished");
					new downloadHandler(selfID, totalChunks, numChunksRecd, barrier1, filename, dwldPort).start();
					new uploadHandler(selfID, totalChunks, barrier1, filename, upldPort).start();
					barrier1.await();
					mergeFile();
					System.out.println ("\nMain Thread: Download Complete.\n");
					clientSock.close();
					System.exit(0);
				}
				else if (inStr.toLowerCase().contains("cnt"))
				{
					int index = inStr.lastIndexOf(":");
					String totalNumChunks = inStr.substring(index + 1); //Assuming that no space is there
					totalChunks = Integer.parseInt(totalNumChunks.trim()); //TODO incorrect value handling
				}
				else if (inStr.toLowerCase().contains("id"))
				{
					int index = inStr.lastIndexOf(":");
					selfID = Integer.parseInt(inStr.substring(index + 1).trim());//TODO incorrect value handling
				}
				else if (inStr.toLowerCase().contains("file"))
				{
					filename = inStr.substring(5);
					//System.out.println ("Filename is " + filename);
				}
			}
			else if (x.getClass().equals(byte[].class)) //File Chunk data
			{
				System.out.println ("Main Thread: Downloaded Chunk = " + recvdChunkNum + ". Bytes Received = " + ((byte[])x).length); //Receives a chunk
				FileOutputStream writeToFile = new FileOutputStream(new File("data/" + filename + ".part" + recvdChunkNum));
				writeToFile.write((byte[])x);
				updateChunkSummary(recvdChunkNum);
				numChunksRecd++;
			}
		}
	}
	
	private void startClient() throws Exception{
		clientSock = new Socket();
		while (true)
		{
			try{
				clientSock = new Socket();
				clientSock.connect(new InetSocketAddress("localhost", srvPort));
				break;
			} catch (Exception ie) {
				clientSock.close();
				System.out.println ("Main Thread: Error Connecting to Server Socket. Retry later"); //TODO Num of tries??
				Thread.sleep (3000);
			}
		}
		
		outStream = new ObjectOutputStream (clientSock.getOutputStream());
		inStream = new ObjectInputStream(clientSock.getInputStream());
		outStream.writeObject("connect");
		System.out.println ("Main Thread: Connected to server on port " + srvPort);
		File dataFile = new File("data/");
		if (!dataFile.exists())
        {
        	dataFile.mkdir();
        }
		recv();
	}
	
	public Client(int srvPort) throws Exception {
		this.srvPort = srvPort;
		barrier1 = new CyclicBarrier(3, barrier1Action);
		startClient();
	}
	
	private static int getServerPortFromConfig() throws Exception{
		File confFile = new File("config.txt");
		if (confFile.exists())
		{
			BufferedReader br = new BufferedReader(new FileReader("config.txt"));
			String line = br.readLine();
			if (line != null)
			{
				return Integer.parseInt(line.substring(2, 6));
			}
		}
		return -1;
	}
	
	public static void main(String[] args) throws Exception{
		int port;
		if (args.length > 0)
		{
			port = Integer.parseInt(args[0]);
		}
		else if ((port = getServerPortFromConfig()) != -1)
		{
			;//Do nothing
		}
		else
		{
			port = 9000; //If no server port is provided, 9000 is assumed 
		}
		Client client = new Client(port);
	}
	
}

class downloadHandler extends Thread{
	int selfID, totalNumChunks, dwldPort, numChunksRequested, numChunksRecvd, totalChunksRecvd;
	Socket clientDwldSock = new Socket();
	Timer reqTimer = new Timer();
	Timer disconnectTimer = new Timer();
	boolean reqTimerPaused = false;
	ObjectOutputStream outStream;
	ObjectInputStream inStream;
	StringBuilder chunksToGet = new StringBuilder();
	String filename;
	CyclicBarrier b;
	
	public downloadHandler(int selfID, int totalNumChunks, int numChunksRecd, CyclicBarrier barrier1, String filename, int dwldPort){
		this.selfID = selfID;
		this.totalNumChunks = totalNumChunks;
		this.totalChunksRecvd = numChunksRecd;
		this.b = barrier1;
		this.filename = filename;
		this.dwldPort = dwldPort;
	}  
	
	private String readChunkSummary() throws Exception {
		File chunkSummary = new File ("chunksummary.txt");
		String line;
		if (chunkSummary.exists()){
			BufferedReader br = new BufferedReader(new FileReader(chunkSummary));
			line = br.readLine();
			return line;
		}
		return null;
	}
	
	private List<Integer> getLocalChunkList() throws Exception {
		File chunkSummary = new File ("chunksummary.txt");
		String line;
		LinkedList<Integer> list = new LinkedList<Integer>();
		if (chunkSummary.exists()) {
			BufferedReader br = new BufferedReader(new FileReader(chunkSummary));
			line = br.readLine();
			StringTokenizer strTok = new StringTokenizer(line.trim());
			String nextTok;
			while (strTok.hasMoreTokens())
			{
				nextTok = strTok.nextToken(" ");
				list.add(Integer.parseInt(nextTok));
			}
			return list;
		}
		return null;
	}

	
	private void updateChunkSummary(int recvdChunkNum) throws Exception {
		//System.out.println ("UpdateChunkSymmary " + recvdChunkNum);
		File chunkSummary = new File ("chunksummary.txt");
    	if(!chunkSummary.exists()){
     	   chunkSummary.createNewFile();
     	}
    	FileWriter fw = new FileWriter(chunkSummary,true);
    	BufferedWriter bw = new BufferedWriter(fw);
    	bw.write(" " + recvdChunkNum);
    	bw.close();
	}
	
	public void run(){
		try{
			//System.out.println ("Download Handler Thread");
			//getConfig();
			//Connect to Download Client
			System.out.println("Download Thread: Connecting to download neighbor on port " + dwldPort + "...");
			while (true) {
				   try {
					   clientDwldSock = new Socket();
					   clientDwldSock.connect(new InetSocketAddress("localhost", dwldPort));
					   System.out.println ("Download Thread: Download Neighbor Connected");
				        break;
				    } catch (Exception e) { 
				    	clientDwldSock.close();
				    	//System.out.print(".");
				    	Thread.sleep(3000);
				    	}
			}
			outStream = new ObjectOutputStream(clientDwldSock.getOutputStream());
			getChunkListFromDwldNeighbor();
			recv();
			
		}catch (SocketException sockE){
			System.out.println ("Download Thread: Remote End Disconnected.");
		}catch (EOFException eof){
			System.out.println ("Download Thread: Remote End Disconnected.");
		}catch (Exception e) {
			System.out.print ("Download Thread:");
			e.printStackTrace();
		}
	}
	
	private int compareChunkList(StringBuilder chunkToDwld, String neighborList ) throws Exception{
		//System.out.println ("Remote Chunk List = " + neighborList);
		//String ourChunkList = new String(readChunkSummary());
		List<Integer> ourChunkList = getLocalChunkList();
		//System.out.println ("Local Chunk List = " + ourChunkList);
		StringTokenizer t = new StringTokenizer (neighborList.trim());
		String nextTok;
		int count = 0;
		while (t.hasMoreTokens())
		{
			nextTok = t.nextToken(" ");
			//System.out.println ("next Tok" + nextTok);
			if (!ourChunkList.contains(Integer.parseInt(nextTok)))
			{
				count++;
				chunkToDwld.append(" " + nextTok);
			}
			/*else
				System.out.println ("remote list contains " + nextTok);*/
		}
		if (count > 0)
		{
			reqTimerPaused = true;
		}
		if (chunkToDwld.length() > 0 )
			System.out.println ("Download Thread: Chunks to download = " + chunkToDwld);
		return count;
	}
	
	private void getChunkListFromDwldNeighbor() throws Exception{
		String msg;
		System.out.println ("Download Thread: Requesting chunk list from Download peer");
		msg = new String ("GET LST");
		outStream.writeObject(msg);
	}
	
	
	private void recv() throws Exception{
		inStream = new ObjectInputStream(clientDwldSock.getInputStream());
		int recvdChunkNum = 0;
		while (true)
		{
			Object x;
			x = inStream.readObject();
			if (x.getClass().equals(String.class))
			{
				String inStr = (String) x;
				//System.out.println ("Download Thread: RECEIVED: " + inStr);
				if (inStr.toLowerCase().contains("lst"))
				{
					System.out.println ("Download Thread: Received Chunk List from Download Peer = " + inStr.substring(4, inStr.length())); //receives a chunk id List
					//This is a chunk list
					if (compareChunkList(chunksToGet, inStr.substring(4, inStr.length())) != 0)
					{
						numChunksRequested = chunksToGet.toString().length() - chunksToGet.toString().replaceAll(" ", "").length();
						//System.out.println ("Num Chunks requested = " + numChunksRequested);
						System.out.println ("Download Thread: Requesting Chunks = " + chunksToGet.toString().trim()); //Requests for chunks
						outStream.writeObject("SEND:" + chunksToGet.toString().trim());
						chunksToGet.delete(0, chunksToGet.length());
					}
				}
				if (inStr.toLowerCase().contains("chnk"))
				{
					int index = inStr.lastIndexOf(":");
					String chunkNum = inStr.substring(index + 1); //Assuming that no space is there
					recvdChunkNum = Integer.parseInt(chunkNum);
				}
			}
			else if (x.getClass().equals(byte[].class)) 
			{
				FileOutputStream writeToFile = new FileOutputStream(new File("data/" + filename + ".part" + recvdChunkNum));
				writeToFile.write((byte[])x);
				updateChunkSummary(recvdChunkNum);
				System.out.println("Download Thread: Downloaded Chunk = " + recvdChunkNum + ". Bytes Received = " + ((byte[])x).length); //receives a chunk
				numChunksRecvd++;
				totalChunksRecvd++;
				//System.out.println ("Download Thread: SENDING =  OKAY");
				if (numChunksRequested == numChunksRecvd)
				{
					//System.out.println ("Schedule timer");
					numChunksRequested = 0;
					numChunksRecvd = 0;
					reqTimerPaused = false;
					reqTimer.schedule(new TimerTask() {
						  public void run() {
							  try{
								  if (!reqTimerPaused)
									  getChunkListFromDwldNeighbor();
							  }
							  catch (Exception e)
							  {
								  System.out.print("Download Thread: ");
								  e.printStackTrace();
							  }
						  }
						}, 0, 5000);
				}
				outStream.writeObject("OKAY:" + recvdChunkNum);
				if (totalChunksRecvd == totalNumChunks)
				{
					System.out.println("Download Thread: Download Finished. Exiting Download Thread.");
					outStream.writeObject("FIN");
					reqTimerPaused = true;
					reqTimer.cancel();
					reqTimer.purge();
					b.await();
					disconnectTimer.schedule(new TimerTask() {
						  public void run() {
							  try{
								  	//inStream.close();
								  	//outStream.close();
									clientDwldSock.close();
							  }
							  catch (Exception e)
							  {
								  System.out.print("Download Thread: ");
								  e.printStackTrace();
							  }
						  }
						}, 5000);
					//reqTimerPaused = false;
				}
			}
		}
	}
}

class uploadHandler extends Thread{
	int selfID, totalNumChunks, upldPort;
	Socket connection = null;
	ObjectOutputStream outStream;
	ObjectInputStream inStream;
	CyclicBarrier b;
	String filename;
	Timer disconnectTimer = new Timer();
	
	public uploadHandler(int selfID, int totalNumChunks, CyclicBarrier barrier1, String filename, int upldPort){
		this.selfID = selfID;
		this.totalNumChunks = totalNumChunks;
		this.b = barrier1;
		this.filename = filename;
		this.upldPort = upldPort;
	} 
	
	private String readChunkSummary() throws Exception {
		File chunkSummary = new File ("chunksummary.txt");
		String line;
		if (chunkSummary.exists()){
			BufferedReader br = new BufferedReader(new FileReader(chunkSummary));
			line = br.readLine();
			return line;
		}
		return null;
	}
	
	public void run(){
		//System.out.println ("Upload Handler Thread");
		//Wait for upload Client to connect
		//Client makes a request, always. 
		//Wait for the client to ask for chunks
		try
		{
			ServerSocket srv = new ServerSocket(upldPort);
			System.out.println("Upload Thread: Waiting for connection on port " + upldPort + "...");
		//accept a connection from the client
			while (true)
			{
				connection = srv.accept();
				if (connection != null)
					break;
			}
			System.out.println ("Upload Thread: Connection Recieved from " + connection.getInetAddress().getHostName() + ":" + connection.getPort());
			inStream = new ObjectInputStream (connection.getInputStream());
			outStream = new ObjectOutputStream (connection.getOutputStream());
			recv();
		}catch (SocketException sockE){
			System.out.println ("Upload Thread: Remote End Disconnected.");
		}catch (EOFException eof){
			System.out.println ("Upload Thread: Remote End Disconnected.");
		}catch (Exception e){
			System.out.print("Upload Thread ");
			e.printStackTrace();
		}
	}
	
	private void recv() throws Exception{
		//ObjectInputStream inStream = new ObjectInputStream(connection.getInputStream());
		String inStr, line;
		while (true)
		{
			outStream.flush();
			inStr = (String)inStream.readObject();
			//System.out.println ("Upload Thread: RECEIVED: " + inStr);
			if (inStr.contains("GET LST"))
			{
				line = readChunkSummary();
				System.out.println ("Upload Thread: Send Chunk ID list = " + line.trim()); //Sends a chunk ID list
				outStream.writeObject("LST:"+ line.trim());
			}
			else if (inStr.contains("SEND"))
			{
				line = inStr.substring(5, inStr.length());
				System.out.println ("Upload Thread: Chunks Requested = " + line);
				StringTokenizer t = new StringTokenizer(line);
				String chunkName, chunkNum;
				while (t.hasMoreTokens())
				{
					chunkNum = t.nextToken(" ");
					chunkName = "data/" + chunkNum;
					//System.out.println ("Filename =" + chunkName);
					byte[] chunk;
					File chunkFile = new File(new String("data/" + filename + ".part"+chunkNum));
					int len;
					FileInputStream chunkIn = new FileInputStream(chunkFile);
					BufferedInputStream inputStream = new BufferedInputStream (chunkIn);
					chunk = new byte[(int)chunkFile.length()];
					if ((len = inputStream.read(chunk)) > 0)
					{
						outStream.writeObject("CHNK:" + chunkNum);
						outStream.writeObject(chunk);
						System.out.println ("Upload Thread: Sent chunk " + chunkNum + ". Bytes Sent = " + len );
					}
				}
			}
			else if (inStr.toLowerCase().contains("okay"))
			{
				int index = inStr.lastIndexOf(":");
				String chunkNum = inStr.substring(index + 1);
				System.out.println ("Upload Thread: Peer successfully received chunk " + chunkNum); 
			}
			else if (inStr.toLowerCase().equals("fin"))
			{
				System.out.println ("\nUpload Thread: Download Peer has downloaded all the chunks and it has the entire file. Safe to exit upload thread.");
				b.await();
				disconnectTimer.schedule(new TimerTask() {
					  public void run() {
						  try{
							  	//inStream.close();
							  	//outStream.close();
							  	connection.close();
						  }
						  catch (Exception e)
						  {
							  System.out.print("Upload Thread: ");
							  e.printStackTrace();
						  }
					  }
					}, 5000);
			}
		}
		
	}
}