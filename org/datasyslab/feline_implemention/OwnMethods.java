package org.datasyslab.feline_implemention;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.neo4j.graphdb.Node;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


public class OwnMethods {
	
	public static void EntityConversionForCopy(String source_filepath, String output_path, String delimiters)
	{
		BufferedReader reader = null;
		File file = null;
		FileWriter fw = null;
		
		try
		{
			file = new File(source_filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			temp = reader.readLine();
			
			fw = new FileWriter(output_path);
			while((temp = reader.readLine())!=null)
			{
				String[] l = temp.split(delimiters);
				if(Integer.parseInt(l[1]) == 1)
				{
					String id = l[0];
					String lon = l[2];
					String lat = l[3];
					String line = String.format("%s\t%s,%s\n", id, lon, lat);
					fw.write(line);
				}
			}
			reader.close();
			fw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			if(fw!=null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	
	public static ArrayList<Long> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/Real_data/"+datasource+"/experiment_id.txt";
		int offset = OwnMethods.GetNodeCount(datasource);
		ArrayList<Long> al = new ArrayList<Long>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(Long.parseLong(temp)+offset);
			}
			reader.close();
		}
		catch(Exception e)
		{
			
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
		return al;
	}
	
	public static void Print(Object o)
	{
		System.out.println(o);
	}
	
	//Print elements in an array
	public static void PrintArray(String[] l)
	{
		for(int i = 0;i<l.length;i++)
			System.out.print(l[i]+"\t");
		System.out.print("\n");
	}
	
	//Generate Random node_count vertices in the range(0, graph_size) which is attribute id
	public static HashSet<String> GenerateRandomInteger(long graph_size, int node_count)
	{
		HashSet<String> ids = new HashSet();
		
		Random random = new Random();
		while(ids.size()<node_count)
		{
			Integer id = (int) (random.nextDouble()*graph_size);
			ids.add(id.toString());
		}
		
		return ids;
	}
	
	
	
	public ArrayList<String> ReadFile(String filename)
	{
		ArrayList<String> lines = new ArrayList<String>();
		
		File file = new File(filename);
		BufferedReader reader = null;
		
		try
		{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine())!=null)
			{
				lines.add(tempString);
			}
			reader.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader!=null)
			{
				try
				{
					reader.close();
				}
				catch(IOException e)
				{					
				}
			}
		}
		return lines;
	}
	
	public static void WriteFile(String filename, boolean app, ArrayList<String> lines)
	{
		try 
		{
			FileWriter fw = new FileWriter(filename,app);
			for(int i = 0;i<lines.size();i++)
			{
				fw.write(lines.get(i)+"\n");
			}
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public static void WriteFile(String filename, boolean app, String str)
	{
		FileWriter fw = null;
		try 
		{
			fw = new FileWriter(filename,app);
			fw.write(str);
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		finally
		{
			if(fw!=null)
				try {
					fw.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}

	public static long getDirSize(File file) {     
        if (file.exists()) {     
            if (file.isDirectory()) {     
                File[] children = file.listFiles();     
                long size = 0;     
                for (File f : children)     
                    size += getDirSize(f);     
                return size;     
            } else {
            	long size = file.length(); 
                return size;     
            }     
        } else {     
            System.out.println("File not exists!");     
            return 0;     
        }     
    }
	
	public static int GetNodeCount(String datasource)
	{
		int node_count = 0;
		File file = null;
		BufferedReader reader = null;
		try
		{
			file = new File("/home/yuhansun/Documents/Real_data/"+datasource+"/graph.txt");
			reader = new BufferedReader(new FileReader(file));
			String str = reader.readLine();
			String[] l = str.split(" ");
			node_count = Integer.parseInt(l[0]);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return node_count;		
	}
	
	public static String ClearCache()
	{
		//String[] command = {"/bin/bash","-c","echo data| sudo -S ls"};
		String []cmd = {"/bin/bash","-c","echo data | sudo -S sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""};
		String result = null;
		try 
		{
			Process process = Runtime.getRuntime().exec(cmd);
			process.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));  
	        StringBuffer sb = new StringBuffer();  
	        String line;  
	        while ((line = br.readLine()) != null) 
	        {  
	            sb.append(line).append("\n");  
	        }  
	        result = sb.toString();
	        result+="\n";
	        
        }   
		catch (Exception e) 
		{  
			e.printStackTrace();
        }
		return result;
	}
	
	
	public static String Serialize_RoarBitmap_ToString(RoaringBitmap r)
	{
		r.runOptimize();
				
		ByteBuffer outbb = ByteBuffer.allocate(r.serializedSizeInBytes());
        // If there were runs of consecutive values, you could
        // call mrb.runOptimize(); to improve compression 
        try {
			r.serialize(new DataOutputStream(new OutputStream(){
			    ByteBuffer mBB;
			    OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
			    public void close() {}
			    public void flush() {}
			    public void write(int b) {
			        mBB.put((byte) b);}
			    public void write(byte[] b) {mBB.put(b);}            
			    public void write(byte[] b, int off, int l) {mBB.put(b,off,l);}
			}.init(outbb)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //
        outbb.flip();
        String serializedstring = Base64.getEncoder().encodeToString(outbb.array());
        return serializedstring;
	}
	
	public static ImmutableRoaringBitmap Deserialize_String_ToRoarBitmap(String serializedstring)
	{
		ByteBuffer newbb = ByteBuffer.wrap(Base64.getDecoder().decode(serializedstring));
	    ImmutableRoaringBitmap ir = new ImmutableRoaringBitmap(newbb);
	    return ir;
	}
	
	public static RoaringBitmap GetRoaringBitmap(String serializedstring)
	{
		RoaringBitmap rb = new RoaringBitmap();
		try
		{
		    byte[] nodeIds = serializedstring.getBytes();
		    ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
		    rb.deserialize(new DataInputStream(bais));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	    return rb;
	}
	
	public static void PrintNode(Node node)
	{
		Iterator<String> iter = node.getPropertyKeys().iterator();
		HashMap<String, String> properties = new HashMap();
		while(iter.hasNext())
		{
			String key = iter.next();
			properties.put(key, node.getProperty(key).toString());
		}
		System.out.println(properties.toString());
	}
	
	public static ArrayList<Integer> ReadTopoSequence(String filepath)
	{
		ArrayList<Integer> seq = null;
		File file = null;
		BufferedReader reader = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String str = reader.readLine();
			int size = Integer.parseInt(str);
			seq = new ArrayList<Integer>(size);
			for(int i = 0;i<size;i++)
				seq.add(0);
			while((str = reader.readLine())!=null)
			{
				String[] l = str.split("\t");
				Integer index = Integer.parseInt(l[0]);
				Integer id = Integer.parseInt(l[1]);
				seq.set(id, index);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return seq;
	}
}
