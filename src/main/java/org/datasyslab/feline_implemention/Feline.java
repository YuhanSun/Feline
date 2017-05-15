package org.datasyslab.feline_implemention;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import javax.naming.spi.DirStateFactory.Result;

import org.omg.PortableServer.ID_ASSIGNMENT_POLICY_ID;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class Feline {
	
	public PostgresJDBC postgresJDBC;
	public String tablename;
	public String location_columnname = "location";
	
	public int hopsTotal;
	public int node_count;
	public int[] visited;
	public int QueryCnt;
	
	//record variable
	public int visited_count = 0;
	public int spa_time = 0;
	public int reach_time = 0;
	public int locate_count = 0;
	
	public Neo4j_Graph_Store neo4j_Graph_Store;
	
	public Feline(int nodeCount, String table_name)
	{
		this.node_count = nodeCount;
		int i;
		visited = new int[node_count];
		for(i = 0 ; i< node_count; i++)
			visited[i]=-1;
		
		neo4j_Graph_Store = new Neo4j_Graph_Store();
		postgresJDBC = new PostgresJDBC();
		this.tablename = table_name;
	}
	
	public Feline(String datasource, String table_name)
	{
		this.node_count = OwnMethods.GetNodeCount(datasource);
		int i;
		visited = new int[node_count];
		for(i = 0 ; i< node_count; i++)
			visited[i]=-1;
		
		neo4j_Graph_Store = new Neo4j_Graph_Store();
		postgresJDBC = new PostgresJDBC();
		this.tablename = table_name;
	}
	
	public Feline(String datasource, String table_name, int dag_node_count)
	{
		this.node_count = dag_node_count;
		int i;
		visited = new int[node_count];
		for(i = 0 ; i< node_count; i++)
			visited[i]=-1;
		
		neo4j_Graph_Store = new Neo4j_Graph_Store();
		postgresJDBC = new PostgresJDBC();
		this.tablename = table_name;
	}
	
	public void Disconnect()
	{
		postgresJDBC.DisConnect();
	}
	
	public ResultSet RangeQuery(MyRectangle rect)
	{
		String query = String.format("select distinct scc_id from %s where %s <@ box '((%f,%f),(%f,%f))'", tablename, location_columnname, rect.min_x, rect.min_y, rect.max_x, rect.max_y);
		ResultSet resultSet = postgresJDBC.Execute(query);
//		OwnMethods.Print(query);
		return resultSet;
	}
	
	public int contains_pc(int src_coord_x, int src_coord_y, int src_middle, int src_post,
						   int trg_coord_x, int trg_coord_y, int trg_middle, int trg_post)
	{
		if(src_coord_x > trg_coord_x)
			return -1;
		if(src_coord_y > trg_coord_y)
			return -1;
		if(src_middle <= trg_middle && src_post >= trg_post)
			return 1;

		return 0;
	}

	public boolean go_for_reach_pc(int src, int trg, boolean LEVEL_FILTER, int hop, int src_level, int trg_level,
			int trg_coord_x, int trg_coord_y, int trg_middle, int trg_post) 
	{
		int res;
		
		if(src==trg)
			return true;
		
		if(src_level >= trg_level)
		{
			return false;
		}

		visited[src] = QueryCnt;
		
		if(hop > hopsTotal)
			hopsTotal = hop;
		
		String query = String.format("match (a)-->(n) where id(a) = %d return id(n), n.level, n.X, n.Y, n.middle, n.post", src);
		String result = neo4j_Graph_Store.Execute(query);
		JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
		
		visited_count += jsonArray.size();
		
		for(int jsonArray_index = 0;jsonArray_index<jsonArray.size();jsonArray_index++)
		{
			JsonObject jsonObject = jsonArray.get(jsonArray_index).getAsJsonObject();
			JsonArray node_row = (JsonArray)jsonObject.get("row");
			int out_id = node_row.get(0).getAsInt();
			if(visited[out_id] != QueryCnt)
			{
				int out_level = node_row.get(1).getAsInt();
				int out_coord_x = node_row.get(2).getAsInt();
				int out_coord_y = node_row.get(3).getAsInt();
				int out_middle = node_row.get(4).getAsInt();
				int out_post = node_row.get(5).getAsInt();
				res = contains_pc(out_coord_x, out_coord_y, out_middle, out_post, trg_coord_x, trg_coord_y, trg_middle, trg_post);
				switch(res)
				{
				case 1:
					return true;
				case 0:
					if (go_for_reach_pc(out_id,trg,LEVEL_FILTER, hop+1, out_level, trg_level, trg_coord_x, trg_coord_y, trg_middle, trg_post))
						return true; 
						break;
					case -1 :  // NegativeCut++; 
						break;	
				}
			}
		}
		return false;
	}
	
	public boolean Reach_pc(int src, int trg)
	{
		if(src == trg){
			return true;
		}

		String query = "";
		try
		{
			query = String.format("match (n) where id(n) in [%d,%d] return n.level, n.X, n.Y, n.middle, n.post", src, trg);
			String result = neo4j_Graph_Store.Execute(query);
			JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);

			JsonArray jsonArray_src = jsonArray.get(0).getAsJsonObject().get("row").getAsJsonArray();
			JsonArray jsonArray_trg = jsonArray.get(1).getAsJsonObject().get("row").getAsJsonArray();
			
			visited_count += 2;

			int src_level = jsonArray_src.get(0).getAsInt();
			int trg_level = jsonArray_trg.get(0).getAsInt();

			if(src_level >= trg_level)
			{
				return false;
			}
			int res = 0;
			
			int src_coord_x = jsonArray_src.get(1).getAsInt();
			int trg_coord_x = jsonArray_trg.get(1).getAsInt();
			if(src_coord_x > trg_coord_x)
				res = -1;
			
			
			int src_coord_y = jsonArray_src.get(2).getAsInt();
			int trg_coord_y = jsonArray_trg.get(2).getAsInt();
			
			if(src_coord_y > trg_coord_y)
				res = -1;
	       	
			int src_middle = jsonArray_src.get(3).getAsInt();
			int src_post = jsonArray_src.get(4).getAsInt();
			int trg_middle = jsonArray_trg.get(3).getAsInt();
			int trg_post = jsonArray_trg.get(4).getAsInt();
			
			if(src_middle <= trg_middle && src_post >= trg_post)
				res = 1;

			if(res!=0){						
				switch(res){
				case -1 : 
					return false; 
				case 1 : 
					return true;
				}
			}
			
			hopsTotal = 0;
			visited[src]=++QueryCnt;
			return go_for_reach_pc(src,trg,true,0, src_level, trg_level, trg_coord_x, trg_coord_y, trg_middle, trg_post);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			OwnMethods.Print(query);
		}
		return false;
	}
	
//	public boolean Reach_pc(int src, ArrayList<Integer> trg_l)
//	{
//		try
//		{
//			String query = String.format("match (n) where id(n) in [%d] return n.level, n.X, n.Y, n.middle, n.post", src);
//			String result = neo4j_Graph_Store.Execute(query);
//			JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//			
//			JsonArray jsonArray_src = jsonArray.get(0).getAsJsonObject().get("row").getAsJsonArray();
//			visited_count+=1;
//			int src_level = jsonArray_src.get(0).getAsInt();
//			
//			query = String.format("match (n) where id(n) in %s return n.level, n.X, n.Y, n.middle, n.post", trg_l.toString());
//			result = neo4j_Graph_Store.Execute(query);
//			jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//			visited_count += jsonArray.size();
//			
//			for(int i = 0;i<jsonArray.size();i++)
//			{
//				JsonArray jsonArray_trg = jsonArray.get(1).getAsJsonObject().get("row").getAsJsonArray();
//				int trg_level = jsonArray_trg.get(0).getAsInt();
//				
//				if(src_level >= trg_level)
//				{
//					continue;
//				}
//				int res = 0;
//				
//				int src_coord_x = jsonArray_src.get(1).getAsInt();
//				int trg_coord_x = jsonArray_trg.get(1).getAsInt();
//				if(src_coord_x > trg_coord_x)
//					res = -1;
//				
//				
//				int src_coord_y = jsonArray_src.get(2).getAsInt();
//				int trg_coord_y = jsonArray_trg.get(2).getAsInt();
//				
//				if(src_coord_y > trg_coord_y)
//					res = -1;
//		       	
//				int src_middle = jsonArray_src.get(3).getAsInt();
//				int src_post = jsonArray_src.get(4).getAsInt();
//				int trg_middle = jsonArray_trg.get(3).getAsInt();
//				int trg_post = jsonArray_trg.get(4).getAsInt();
//				
//				if(src_middle <= trg_middle && src_post >= trg_post)
//					res = 1;
//
//				if(res!=0){						
//					switch(res){
//					case -1 : 
//						continue;
//					case 1 : 
//						return true;
//					}
//				}
//			}
//			
//			hopsTotal = 0;
//			visited[src]=++QueryCnt;
//			return go_for_reach_pc(src,trg,true,0, src_level, trg_level, trg_coord_x, trg_coord_y, trg_middle, trg_post);
//		}
//		catch(Exception e)
//		{
//			e.printStackTrace();
//		}
//		return false;
//	}
	
	public boolean RangeReach(int id, MyRectangle rect)
	{
		ResultSet resultSet = null;
		try
		{
			visited_count = 0;
			spa_time = 0;
			reach_time = 0;
			locate_count = 0;
			
			long start = System.currentTimeMillis();
			resultSet = this.RangeQuery(rect);
			spa_time += System.currentTimeMillis() - start;
			
			start = System.currentTimeMillis();
			while(resultSet.next())
			{
				locate_count ++;
				int trg = (resultSet.getInt("scc_id"));
				if(Reach_pc(id, trg))
				{
					reach_time += System.currentTimeMillis() - start;
					return true;
				}
				else
					continue;
			}
			reach_time += System.currentTimeMillis() - start;
			return false;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally{
			PostgresJDBC.Close(resultSet);
		}
		OwnMethods.Print(String.format("Error in RangeReach query: %d (%f,%f,%f,%f)", id, rect.min_x, rect.min_y, rect.max_x, rect.max_y));
		return false;
	}
	
//	public static void Reach_Correctness()
//	{
//		String testfile = String.format("/home/yuhansun/Downloads/tests1/cit-Patents_500k.test");
//		BufferedReader reader = null;
//		BufferedReader reader_result = null;
//		Feline feline = new Feline("Patents","");
//		Neo4j_Graph_Store neo4j_Graph_Store = new Neo4j_Graph_Store();
//		try
//		{
//			//read result file
//			String line = null;
//			String result_path = "/home/yuhansun/Documents/share/result.txt";
//			reader_result = new BufferedReader(new FileReader(new File(result_path)));
//			ArrayList<Integer> results = new ArrayList<Integer>();
//			while((line = reader_result.readLine()) != null)
//			{
//				int result = Integer.parseInt(line);
//				results.add(result);
//			}
//			reader_result.close();
//			
//			reader = new BufferedReader(new FileReader(new File(testfile)));
//			
//			int i = 0;
//			while((line = reader.readLine()) != null)
//			{
//				OwnMethods.Print(i);
//				String[] line_list = line.split(" ");
//				int src = Integer.parseInt(line_list[0]);
//				int trg = Integer.parseInt(line_list[1]);
//				boolean result1 = feline.Reach_pc(src, trg);
//				OwnMethods.Print(result1);
//				
////				String query = String.format("match p = (a)-[*]->(b) where id(a) = %d and id(b) = %d return p limit 1", src, trg);
////				String result = neo4j_Graph_Store.Execute(query);
////				JsonArray jsonArray = Neo4j_Graph_Store.GetExecuteResultDataASJsonArray(result);
//				
//				i++;
////				if(result1 && (jsonArray.size() == 1) || ((!result1) && (jsonArray.size() == 0)))
//				if(result1 && (results.get(i-1) == 1) || ((!result1) && results.get(i-1) == 0))
//					continue;
//				else
//				{
//					OwnMethods.Print(String.format("%d, %d", src, trg));
//					return;
//				}
//				
//			}
//			reader.close();
//		}
//		catch (Exception e)
//		{
//			e.printStackTrace();
//		}
//		
//	}
	
//	public static void RangeReachTest()
//	{
//		String datasource = "Patents";
//		String table_name = String.format("feline_%s_random_80", datasource);
//		Traversal traversal = new Traversal();
//		Feline feline = new Feline(datasource, table_name);
//				
//		int graph_size = OwnMethods.GetNodeCount(datasource);
//		
//		double rect_size = 1;
//		boolean break_flag = false;
//		while(rect_size<=101)
//		{
//			HashSet<String> hSet = null;
////			HashSet<String> hSet = OwnMethods.GenerateRandomInteger(graph_size, 500);
//			Iterator<String> iterator = hSet.iterator();
//			int i = 0;
//			int true_count = 0;
//			while(iterator.hasNext())
//			{
//				Random random = new Random();
//				double x = random.nextDouble()*(1000 - rect_size);
//				double y = random.nextDouble()*(1000 - rect_size);
//				MyRectangle rect = new MyRectangle(x,y, x+rect_size, y+rect_size);
//				
//				OwnMethods.Print(i);
//				int id = Integer.parseInt(iterator.next());
//				boolean result1 = traversal.ReachabilityQuery(id, rect);
//				OwnMethods.Print(result1);
//				boolean result2 = feline.RangeReach(id, rect);
//				OwnMethods.Print(result2);
//				
//				if(result1 != result2)
//				{
//					OwnMethods.Print(String.format("error in %s, (%f,%f,%f,%f)", id, rect.min_x, rect.min_y, rect.max_x, rect.max_y));
//					break_flag = true;
//					break;
//				}
//				
//				if(result1)
//					true_count++;
//				i++;
//			}
//			OwnMethods.Print(String.format("Total count: %d, true count: %d", i, true_count));
//			if(break_flag)
//				break;
//			rect_size *= 10;
//		}
//		
//		feline.Disconnect();
//		
//	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		Reach_Correctness();
//		RangeReachTest();
		
//		String datasource = "Patents";
//		String tablename = String.format("feline_%s_random_80", datasource);
//		String location_columnname = "location";
//		Feline feline = new Feline(datasource, tablename);
//		
//		MyRectangle rect = new MyRectangle(0,0,4,4);
//		ResultSet rSet = feline.RangeQuery(rect);
//		try {
//			while(rSet.next())
//			{
//				OwnMethods.Print(rSet.getString("id"));
//			}
//			rSet.close();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		feline.Disconnect();
		
//		OwnMethods.Print(feline.Reach_pc(704168, 3557024));
	}

}
