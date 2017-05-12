package org.datasyslab.feline_implemention;

import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App 
{
	public static void ChangeString(String string)
	{
//		string.;
//		OwnMethods.Print(string);
	}
	
    public static void main( String[] args )
    {
//    	int start = 153933;
//		MyRectangle query_rect = new MyRectangle(-115.487384,	36.047015,	-115.095464,	36.120295);
//		
//		Feline feline = new Feline("Yelp", "Yelp_2", 486680);
//		boolean result = feline.RangeReach(start, query_rect);
//		OwnMethods.Print(result);
    	
    	SpaReachPLL.ExperimentColdPostgresNeo4j(2);
    	Experiment_feline.ExperimentColdPostgresNeo4j(2);
		
//    	ArrayList<Integer> tt = new ArrayList<Integer>(){
//    		{
//    			add(0);
//    			add(2);
//    		}
//    	};
//    	OwnMethods.Print(tt.toString());
    	
//    	Postgres_Operation psql = null;
//    	try
//    	{
//    		psql = new Postgres_Operation();
//    		for(int ratio = 20; ratio<=80; ratio+=20)
//    		{
//    			OwnMethods.ClearCache();
//    			String table_name = String.format("patents_random_%d", ratio);
//    			long start = System.currentTimeMillis();
//    			psql.CreateGistIndex(table_name, "location");
//    			OwnMethods.Print(System.currentTimeMillis() - start + "\n");
//    			
////    			String index_name = String.format("patents_random_%d_location_gist", ratio);
////    			psql.DropIndex(index_name);
//    			
//    		}
//    	}
//    	catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}
//    	finally
//    	{
//    		psql.DisConnect();
//    	}

    	//        String string = "0: 0 1 1 { -1 -1 -1 }  [ 1,11 ] ";
    	//        OwnMethods.Print(string.split(" ")[11]);
    	//        String query = "query";
    	//        query = new String("query");
    	//        ChangeString(query);
    	//        OwnMethods.Print(query);

    	//    	OwnMethods.Print(OwnMethods.ClearCache());

    	//    	OwnMethods.Print(String.format("%f", 1.0/500));

    	//    	String string1 = "A";
    	//    	String string2 = string1;
    	//    	OwnMethods.Print(string1);
    	//    	OwnMethods.Print(string2);
    	//    	
    	//    	string1 = "B"
    }
}
