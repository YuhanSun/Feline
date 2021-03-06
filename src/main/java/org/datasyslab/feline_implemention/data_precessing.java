package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;

import org.datasyslab.feline_implemention.Config.Distribution;

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
//		add("Random_spatial_distributed");
		add("Clustered_distributed");
		add("Zipf_distributed");
	}};

	//feline format
//	public static void Graph_Convert(String my_graph_path, String new_graph_path)
//	{
//		File file = null;
//		BufferedReader reader = null;
//		FileWriter fw = null;
//		String temp = null;
//		
//		try
//		{
//			file = new File(my_graph_path);
//			reader = new BufferedReader(new FileReader(file));
//			fw = new FileWriter(new_graph_path, false);
//			
//			temp = reader.readLine();
//			int node_count = Integer.parseInt(temp);
//			fw.write("graph_for_greach\n");
//			fw.write(node_count+"\n");
//			
//			while((temp = reader.readLine())!=null)
//			{
//				String[] line = temp.split(",");
//				
//				int id = Integer.parseInt(line[0]);
//				int count = Integer.parseInt(line[1]);
//				String write_line = String.format("%d: ", id);
//				for(int i = 0;i<count;i++)
//					write_line += line[i+2]+" ";
//				write_line += "#\n";
//				fw.write(write_line);
//			}
//			reader.close();
//			fw.close();
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//		finally {
//			try
//			{
//				if(reader != null)
//					reader.close();
//				if(fw != null)
//					fw.close();
//			}
//			catch(Exception e)
//			{
//				e.printStackTrace();
//			}
//			
//		}
//		
//	}
//	
//	public static void Graph_Convert()
//	{
//		for (String datasource : datasource_a)
//		{
//			String my_graph_path = String.format("/home/yuhansun/Documents/Real_data/%s/graph.txt", datasource);
//			String new_graph_path = String.format("/home/yuhansun/Documents/FELINE_final/%s.gra", datasource);
//			OwnMethods.Print("Convert "+datasource);
//			Graph_Convert(my_graph_path, new_graph_path);
//		}
//	}
//	
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
	
	/** 
	 * convert entity_newformat to id scc_id (lon, lat)
	 * @param entity_location_path
	 * @param new_file_path
	 */
	public static void Entity_Convert(String entity_location_path, String new_file_path) 
	{
		File file_location = null;
		BufferedReader reader_location = null;
		FileWriter fWriter = null;
		
		try
		{
			file_location = new File(entity_location_path);
			reader_location = new BufferedReader(new FileReader(file_location));
			String line_location = reader_location.readLine();
			
			fWriter = new FileWriter(new_file_path, false);
			
			while(((line_location = reader_location.readLine())!=null))
			{
				String[] line_list_location = line_location.split(",");
				
				int id = Integer.parseInt(line_list_location[0]);
				
				int isspatial = Integer.parseInt(line_list_location[1]);
				
				if(isspatial == 1)
				{
					String write_line = "";
					write_line += String.format("%d %d ", id, id);
					String lon = line_list_location[2];
					String lat = line_list_location[3];
					write_line += String.format("(%s,%s)\n", lon, lat);
					fWriter.write(write_line);
				}
				
				
			}
			
			reader_location.close();
			fWriter.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				if(reader_location != null)
					reader_location.close();

				if(fWriter != null)
					fWriter.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
//	
//	public static void Entity_Convert()
//	{
////		for (String datasource : datasource_a)
//		String datasource = "Yelp";
//		{
////			for (String distribution : distribution_a)
//			String distribution = "Random_spatial_distributed";
//			{
//				int ratio = 80;
////				for(int ratio = 20; ratio<=80; ratio+=20)
//				{
//					String entity_location_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/entity.txt", datasource, distribution, ratio);
//					String feline_index_path = String.format("/home/yuhansun/Documents/FELINE_final/%s_feline.out", datasource);
//					String new_file_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/entity_feline.txt", datasource, distribution, ratio);
//					Entity_Convert(entity_location_path, feline_index_path, new_file_path);
//				}
//			}
//		}
//	}
//	
//	public static void YelpConvert()
//	{
//		String datasource = "Yelp";
//		String distribution= "Random_spatial_distributed";
////		int ratio = 80;
//		int target_folder = 3;
//		String entity_location_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/new_entity.txt", "Yelp", distribution, ratio); 
//		String newfile_path = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/entity_psql.txt", "Yelp", distribution, ratio);
//		Entity_Convert(entity_location_path, newfile_path);
//		
////		String directory = String.format("/home/yuhansun/Documents/share/Real_Data/%s/%s/%d/", datasource, distribution, target_folder);
////		String ori_graph_path = directory + "graph_dag_newformat.txt";
////		String feline_graph_path = String.format("/home/yuhansun/Documents/FELINE_final/%s_%d.txt", datasource, target_folder);
////	
////		Graph_Convert(ori_graph_path, feline_graph_path);
//		
//	}
	
	/**
	 * convert entity to psql format which can be used for either Feline and PLL
	 */
	public static void convertRealEntityRatio()
	{
		ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("uniprotenc_150m", "Patents", "go_uniprot", "citeseerx"));
		String distribution= Distribution.Random_spatial_distributed.name();
		for ( String datasource : datasource_a)
		{
			for ( int ratio = 20; ratio < 90; ratio += 20)
			{
				String entity_location_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%d/new_entity.txt", 
						datasource, distribution, ratio); 
				String newfile_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%d/entity_psql.txt",
						datasource, distribution, ratio);
				OwnMethods.Print(String.format("convert from %s to \n %s", entity_location_path, newfile_path));
				Entity_Convert(entity_location_path, newfile_path);
			}
		}
	}
	
	/**
	 * convert entity to psql format which can be used for either Feline and PLL
	 * run after convertRealEntityRatio
	 */
	public static void convertRealEntityDistribution()
	{
		int ratio = 20;
		ArrayList<String> datasource_a = new ArrayList<String>(Arrays.asList("uniprotenc_150m", "Patents", "go_uniprot", "citeseerx"));
		for ( String datasource : datasource_a)
		{
			for ( String distribution : distribution_a)
			{
				String entity_location_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%d/new_entity.txt", 
						datasource, distribution, ratio); 
				String newfile_path = String.format("/mnt/hgfs/Ubuntu_shared/Real_Data/%s/%s/%d/entity_psql.txt",
						datasource, distribution, ratio);
				OwnMethods.Print(String.format("convert from %s to \n %s", entity_location_path, newfile_path));
				Entity_Convert(entity_location_path, newfile_path);
			}
		}
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
//		convertRealEntityRatio();
		convertRealEntityDistribution();
		
//		Graph_Convert();
//		Entity_Convert();
//		YelpConvert();
		
//		String entity_location_path = "/home/yuhansun/Documents/Real_data/citeseerx/Random_spatial_distributed/20/entity.txt";
//		String feline_index_path = "/home/yuhansun/Documents/FELINE_final/citeseerx_feline.out";
//		String new_file_path = "/home/yuhansun/Documents/Real_data/citeseerx/Random_spatial_distributed/20/entity_feline.txt";
//		Entity_Convert(entity_location_path, feline_index_path, new_file_path);
	}

}
