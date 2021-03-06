package org.datasyslab.feline_implemention;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.impl.batchimport.ReadRelationshipCountsDataStep;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import com.sun.jersey.api.client.WebResource;

public class OwnMethods {
	public static int GetSpatialEntityCount(ArrayList<Entity> entities)
    {
    	int count = 0;
    	for ( Entity entity : entities)
    		if(entity.IsSpatial)
    			count++;
    	return count;
    }
	
	public static ArrayList<Entity> ReadEntity(String entity_path) {
        ArrayList<Entity> entities = null;
        BufferedReader reader = null;
        String str = null;
        try {
            reader = new BufferedReader(new FileReader(new File(entity_path)));
            str = reader.readLine();
            int node_count = Integer.parseInt(str);
            entities = new ArrayList<Entity>(node_count);
            int id = 0;
            while ((str = reader.readLine()) != null) {
                Entity entity;
                String[] str_l = str.split(",");
                int flag = Integer.parseInt(str_l[1]);
                if (flag == 0) {
                    entity = new Entity();
                    entities.add(entity);
                } else {
                    entity = new Entity(Double.parseDouble(str_l[2]), Double.parseDouble(str_l[3]));
                    entities.add(entity);
                }
                ++id;
            }
            reader.close();
        }
        catch (Exception e) {
        	e.printStackTrace();
        	System.exit(-1);
        }
        return entities;
    }
	
	/**
	 * read integer arraylist
	 * @param path
	 * @return
	 */
	public static ArrayList<Integer> readIntegerArray(String path)
	{
		String line = null;
		ArrayList<Integer> arrayList = new ArrayList<Integer>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
			while ( (line = reader.readLine()) != null )
			{
				int x = Integer.parseInt(line);
				arrayList.add(x);
			}
			reader.close();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return arrayList;
	}
	
	public static ArrayList<MyRectangle> ReadExperimentQueryRectangle(String filepath) {
		ArrayList<MyRectangle> queryrectangles;
		block13 : {
			queryrectangles = new ArrayList<MyRectangle>();
			BufferedReader reader = null;
			File file = null;
			try {
				try {
					file = new File(filepath);
					reader = new BufferedReader(new FileReader(file));
					String temp = null;
					while ((temp = reader.readLine()) != null) {
						String[] line_list = temp.split("\t");
						MyRectangle rect = new MyRectangle(Double.parseDouble(line_list[0]), Double.parseDouble(line_list[1]), Double.parseDouble(line_list[2]), Double.parseDouble(line_list[3]));
						queryrectangles.add(rect);
					}
					reader.close();
				}
				catch (Exception e) {
					e.printStackTrace();
					if (reader == null) break block13;
					try {
						reader.close();
					}
					catch (IOException var8_8) {}
				}
			}
			finally {
				if (reader != null) {
					try {
						reader.close();
					}
					catch (IOException var8_10) {}
				}
			}
		}
		return queryrectangles;
	}
	
	public static ArrayList<String> ReadExperimentNode(String datasource)
	{
		String filepath = "/home/yuhansun/Documents/share/Real_Data/"+datasource+"/experiment_id.txt";
		ArrayList<String> al = new ArrayList<String>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(temp);
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
	
	public static ArrayList<String> ReadExperimentNodeGeneral(String filepath)
	{
		ArrayList<String> al = new ArrayList<String>();
		BufferedReader reader  = null;
		File file = null;
		try
		{
			file = new File(filepath);
			reader = new BufferedReader(new FileReader(file));
			String temp = null;
			while((temp = reader.readLine())!=null)
			{
				al.add(temp);
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
	
	public static ArrayList<Integer> ReadSCC(String SCC_filepath, String original_graph_path)
	{
		ArrayList<Integer> list = null;
		BufferedReader reader;
		String string = null;
		try
		{
			reader = new BufferedReader(new FileReader(new File(SCC_filepath)));
			string = reader.readLine();
			
			long node_count = OwnMethods.GetNodeCountGeneral(original_graph_path);
			list = new ArrayList();
			for(long i = 0;i<node_count;i++)
				list.add((Integer) 0);
			
			Integer scc_id = 0;
			while((string = reader.readLine())!=null)
			{
				string = string.substring(1, string.length() - 1);
				String[] lString = string.split(", ");
				for(int i = 0;i<lString.length;i++)
				{
					long ori_id = Long.parseLong(lString[i]);
					list.set((int) ori_id, scc_id);
				}
				scc_id += 1;
			}
			reader.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return list;
	}
	
	public static long GeoReachIndexSize(String GeoReach_filepath)
	{
		BufferedReader reader_GeoReach = null;
		File file_GeoReach = null;
		long bits = 0;
		try
		{
			file_GeoReach = new File(GeoReach_filepath);
			reader_GeoReach = new BufferedReader(new FileReader(file_GeoReach));
			String tempString_GeoReach = null;

			while((tempString_GeoReach = reader_GeoReach.readLine())!= null)
			{
				String[] l_GeoReach = tempString_GeoReach.split(",");
				
				int type = Integer.parseInt(l_GeoReach[1]);
				switch (type)
				{
				case 0:
					RoaringBitmap r = new RoaringBitmap();
					for(int i = 2;i<l_GeoReach.length;i++)
					{
						int out_neighbor = Integer.parseInt(l_GeoReach[i]);
						r.add(out_neighbor);
					}
					String bitmap_ser = OwnMethods.Serialize_RoarBitmap_ToString(r);
					bits += bitmap_ser.getBytes().length * 8;
					break;
				case 1:
					bits += 32 * 4;
					break;
				case 2:
					bits += 1;
					break;
				}
			}
			reader_GeoReach.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader_GeoReach!=null)
			{
				try
				{
					reader_GeoReach.close();
				}
				catch(IOException e)
				{	
					e.printStackTrace();
				}
			}
		}
		return bits / 8;
	}
	
	//Print elements in an array
	public static void PrintArray(String[] l)
	{
		for(int i = 0;i<l.length;i++)
			System.out.print(l[i]+"\t");
		System.out.print("\n");
	}
	
	public static void Print(Object o)
	{
		System.out.println(o);
	}
	
	//Generate Random node_count vertices in the range(0, graph_size) which is attribute id
	public static HashSet<Long> GenerateRandomInteger(long graph_size, int node_count)
	{
		HashSet<Long> ids = new HashSet();
		
		Random random = new Random();
		while(ids.size()<node_count)
		{
			Long id = (long) (random.nextDouble()*graph_size);
			ids.add(id);
		}
		
		return ids;
	}
	
	//Generate absolute id in database depends on attribute_id and node label
	public static ArrayList<String> GenerateStartNode(WebResource resource, HashSet<String> attribute_ids, String label)
	{
		String query = "match (a:" + label + ") where a.id in " + attribute_ids.toString() + " return id(a)";
		String result = Neo4j_Graph_Store.Execute(resource, query);
		ArrayList<String> graph_ids = Neo4j_Graph_Store.GetExecuteResultData(result);
		return graph_ids;
	}
	
	//Generate absolute id in database depends on attribute_id and node label
	public static ArrayList<String> GenerateStartNode(HashSet<String> attribute_ids, String label)
	{
		Neo4j_Graph_Store p_neo4j_graph_store = new Neo4j_Graph_Store();
		String query = "match (a:" + label + ") where a.id in " + attribute_ids.toString() + " return id(a)";
		String result = p_neo4j_graph_store.Execute(query);
		ArrayList<String> graph_ids = Neo4j_Graph_Store.GetExecuteResultData(result);
		return graph_ids;
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
		try 
		{
			FileWriter fw = new FileWriter(filename,app);
			fw.write(str);
			fw.close();
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
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
	
	public static int GetNodeCountGeneral(String filepath)
	{
		int node_count = 0;
		File file = null;
		BufferedReader reader = null;
		try
		{
			file = new File(filepath);
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
	
	public static String ClearCache(String password)
	{
		//String[] command = {"/bin/bash","-c","echo data| sudo -S ls"};
		String []cmd = {"/bin/bash","-c","echo "+password+" | sudo -S sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""};
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
	
//	public static String RestartMyNeo4jServerClearCache(String datasource)
//	{
//		String result = "";
//		result += Neo4j_Graph_Store.StopMyServer(datasource);
//		result += ClearCache();
//		result += Neo4j_Graph_Store.StartMyServer(datasource);
//		return result;
//	}
	
//	public static String RestartNeo4jServerClearCache(String neo4j_path)
//	{
//		String result = "";
//		result += Neo4j_Graph_Store.StopServer(neo4j_path);
//		result += ClearCache();
//		result += Neo4j_Graph_Store.StartServer(neo4j_path);
//		return result;
//	}
	
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
}