package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class Neo4j_Graph_Loader {
	
	public static ArrayList<String> datasource_a = new ArrayList<String>(){{
//	    add("citeseerx");
	    add("go_uniprot");
	    add("Patents");
	    add("uniprotenc_150m");
//		add("uniprotenc_22m");
//	    add("uniprotenc_100m");
	    
	}};
	
	public static String db_folder_name = "neo4j-community-2.3.3_feline";
	public static String longitude_property_name = "lon";
	public static String latitude_property_name = "lat";
	
	public static void LoadFelineGraph(String entity_path, String graph_path, String db_path)
	{
		OwnMethods.Print(String.format("Load Graph from %s\n%s\nto %s", entity_path, graph_path, db_path));
		
		BatchInserter inserter = null;
		Label graph_label = DynamicLabel.label("Graph");
		Map<String, String> config = new HashMap<String, String>();
		config.put("dbms.pagecache.memory", "6g");
		
		File file = null;
		BufferedReader reader_entity = null;
		BufferedReader reader_graph = null;
		RelationshipType graph_rel = DynamicRelationshipType.withName("LINK");
		
		try
		{
			inserter = BatchInserters.inserter(new File(db_path).getAbsolutePath(), config);
			
			reader_entity = new BufferedReader(new FileReader(new File(entity_path)));

			String line = "";
			while((line = reader_entity.readLine())!=null)
			{
				Map<String, Object> properties = new HashMap<String, Object>();
				String[] line_list = line.split(" ");
				int id = Integer.parseInt(line_list[0]);
				properties.put("id", id);
				
				String location = line_list[1];
				if(!location.equals("null"))
				{
					String[] location_list = location.split(",");
					double lon = Double.parseDouble(location_list[0]);
					double lat = Double.parseDouble(location_list[1]);
					properties.put(longitude_property_name, lon);
					properties.put(latitude_property_name, lat); 
				}
				
				int level = Integer.parseInt(line_list[2]);
				int X = Integer.parseInt(line_list[3]);
				int Y = Integer.parseInt(line_list[4]);
				int middle = Integer.parseInt(line_list[5]);
				int post = Integer.parseInt(line_list[6]);
				properties.put("level", level);
				properties.put("X", X);
				properties.put("Y", Y);
				properties.put("middle", middle);
				properties.put("post", post);
				

				inserter.createNode(id, properties, graph_label);
			}
			reader_entity.close();
			
			//graph relationships
			file = new File(graph_path);										
			reader_graph = new BufferedReader(new FileReader(file));
			line = "";
			reader_graph.readLine();
			while((line = reader_graph.readLine())!=null)
			{	
				String[] l = line.split(" ");
				long start = Long.parseLong(l[0]);
				long count = Long.parseLong(l[1]);
				if(count == 0)
					continue;
				for(int i = 2;i<l.length ;i++)
				{
					long end = Long.parseLong(l[i]);
					inserter.createRelationship(start, end, graph_rel, null);
				}
			}
			reader_graph.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			if(reader_entity!=null)
			{
				try
				{
					reader_entity.close();
				}
				catch(IOException e)
				{					
				}
			}
			if(inserter!=null)
				inserter.shutdown();

			if(reader_entity!=null)
				try {
					reader_entity.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			if(reader_graph!=null)
				try {
					reader_graph.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	
	public static void LoadFelineGraph()
	{
		for (String datasource : datasource_a)
		{
			String distribution = "Random_spatial_distributed", suffix = "random";
			int ratio = 80;
			
			String entity_path = String.format("/home/yuhansun/Documents/Real_data/%s/%s/%d/entity_feline.txt", datasource, distribution, ratio);
			String graph_path = String.format("/home/yuhansun/Documents/Real_data/%s/graph.txt", datasource);
			String db_path = String.format("/home/yuhansun/Documents/Real_data/%s/%s/data/graph.db", datasource, db_folder_name);
			LoadFelineGraph(entity_path, graph_path, db_path);
			
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadFelineGraph();
	}

}
