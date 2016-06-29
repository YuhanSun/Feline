package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.ArrayList;

public class data_precessing {
	
	public static ArrayList<String> datasource_a = new ArrayList<String>(){{
	    add("citeseerx");
	    add("go_uniprot");
	    add("Patents");
	    add("uniprotenc_150m");
//		add("uniprotenc_22m");
//	    add("uniprotenc_100m");
	    
	}};
	
	public static ArrayList<String> distribution_a = new ArrayList<String>(){{
		add("Random_spatial_distributed");
//		add("Clustered_distributed");
//		add("Zipf_distributed");
	}};

	public static void Graph_Convert(String my_graph_path, String new_graph_path)
	{
		File file = null;
		BufferedReader reader = null;
		FileWriter fw = null;
		String temp = null;
		
		try
		{
			file = new File(my_graph_path);
			reader = new BufferedReader(new FileReader(file));
			fw = new FileWriter(new_graph_path, false);
			
			temp = reader.readLine();
			int node_count = Integer.parseInt(temp);
			fw.write("graph_for_greach\n");
			fw.write(node_count+"\n");
			
			while((temp = reader.readLine())!=null)
			{
				String[] line = temp.split(" ");
				
				int id = Integer.parseInt(line[0]);
				int count = Integer.parseInt(line[1]);
				String write_line = String.format("%d: ", id);
				for(int i = 0;i<count;i++)
					write_line += line[i+2]+" ";
				write_line += "#\n";
				fw.write(write_line);
			}
			reader.close();
			fw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally {
			try
			{
				if(reader != null)
					reader.close();
				if(fw != null)
					fw.close();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			
		}
		
	}
	
	public static void Graph_Convert()
	{
		for (String datasource : datasource_a)
		{
			String my_graph_path = String.format("/home/yuhansun/Documents/Real_data/%s/graph.txt", datasource);
			String new_graph_path = String.format("/home/yuhansun/Documents/FELINE_final/%s.gra", datasource);
			OwnMethods.Print("Convert "+datasource);
			Graph_Convert(my_graph_path, new_graph_path);
		}
	}
	
	public static void Entity_Convert(String entity_location_path, String feline_index_path, String new_file_path) 
	{
		File file_location = null, file_feline = null;
		BufferedReader reader_location = null, reader_feline = null;
		FileWriter fWriter = null;
		
		try
		{
			file_location = new File(entity_location_path);
			file_feline = new File(feline_index_path);
			reader_location = new BufferedReader(new FileReader(file_location));
			reader_feline = new BufferedReader(new FileReader(file_feline));
			String line_location = reader_location.readLine();
			String line_feline = reader_feline.readLine();
			
			int node_count_location = Integer.parseInt(line_location.split(" ")[0]);
			int node_count_feline = Integer.parseInt(line_feline.split(" ")[1]);
			if(node_count_location != node_count_feline)
			{
				OwnMethods.Print("Node count inconsistent!");
				return;
			}
			
			fWriter = new FileWriter(new_file_path, false);
			
			reader_feline.readLine();
			int id = 0;
			while(((line_feline = reader_feline.readLine())!=null) && ((line_location = reader_location.readLine())!=null))
			{
				String[] line_list_location = line_location.split(" ");
				String[] line_list_feline = line_feline.split(" ");
				String write_line = "";//id | location(lon,lat) | level | X | Y | middle | post
				
				String id_location = line_list_location[0]+":";
				String id_feline = line_list_feline[0];
				if(!id_location.equals(id_feline))
				{
					OwnMethods.Print(String.format("id %s %s inconsistent!", id_location, id_feline));
					return;
				}
				String level = line_list_feline[1];
				String X = line_list_feline[2];
				String Y = line_list_feline[3];
				
				String[] pc_list = line_list_feline[11].split(",");
				String middle = pc_list[0];
				String post = pc_list[1];
				
				int isspatial = Integer.parseInt(line_list_location[1]);
				if(isspatial == 1)
				{
					String lon = line_list_location[2];
					String lat = line_list_location[3];
					
					write_line = String.format("%d %s,%s %s %s %s %s %s\n", id, lon, lat, level, X, Y, middle, post);
				}
				else
					write_line = String.format("%d null %s %s %s %s %s\n", id, level, X, Y, middle, post);
				
				fWriter.write(write_line);
				id++;
			}
			
			reader_location.close();
			reader_feline.close();
			fWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(reader_location != null)
					reader_location.close();

				if(reader_feline != null)
					reader_feline.close();
				
				if(fWriter != null)
					fWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void Entity_Convert()
	{
		for (String datasource : datasource_a)
		{
			for (String distribution : distribution_a)
			{
				for(int ratio = 80; ratio<=80; ratio+=20)
				{
					String entity_location_path = String.format("/home/yuhansun/Documents/Real_data/%s/%s/%d/entity.txt", datasource, distribution, ratio);
					String feline_index_path = String.format("/home/yuhansun/Documents/FELINE_final/%s_feline.out", datasource);
					String new_file_path = String.format("/home/yuhansun/Documents/Real_data/%s/%s/%d/entity_feline.txt", datasource, distribution, ratio);
					Entity_Convert(entity_location_path, feline_index_path, new_file_path);
				}
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Graph_Convert();
		Entity_Convert();
		
		
//		String entity_location_path = "/home/yuhansun/Documents/Real_data/citeseerx/Random_spatial_distributed/20/entity.txt";
//		String feline_index_path = "/home/yuhansun/Documents/FELINE_final/citeseerx_feline.out";
//		String new_file_path = "/home/yuhansun/Documents/Real_data/citeseerx/Random_spatial_distributed/20/entity_feline.txt";
//		Entity_Convert(entity_location_path, feline_index_path, new_file_path);
	}

}
