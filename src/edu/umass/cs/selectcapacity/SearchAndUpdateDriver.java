package edu.umass.cs.selectcapacity;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gnsclient.client.GNSClient;

/**
 * This is the driver class to perform the GNS based CNS select and update
 * capacity experiment.
 * 
 * @author ayadav
 *
 */
public class SearchAndUpdateDriver
{	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.0;
	
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.0;
	
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.0;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	
	public static final double ATTR_MIN 						= 1.0;
	public static final double ATTR_MAX 						= 1500.0;
	
	public static final String ATTR_PREFIX						= "attr";
	
	public static String ALIAS_PREFIX							= "UserGUID";
	public static final String ALIAS_SUFFIX						= "@gmail.com";
	
	// 100 seconds, experiment runs for 100 seconds
	public static 	 long experimentTime						= 100000;
	
	public static double numUsers 								= -1;
	
	public static ExecutorService taskES;
	
	public static int myID;
	
	public static GNSClient gnsClient;
	
	// per sec
	public static double initRate								= 1.0;
	public static double requestRate							= 1.0; //about every 300 sec
	
	public static int numAttrs									= 1;
	
	public static int numAttrsInQuery							= 1;
	
	public static double rhoValue								= 0.5;
	
	public static boolean userInitEnable						= true;
	
	public static double predicateLength						= 0.5;
	
	// if set to true, the get requests are sent 
	// instead of search.
	public static boolean getEnabled							= false;
	
	public static void main( String[] args ) throws Exception
	{
		myID 			  = Integer.parseInt(args[0]);
		numUsers 		  = Double.parseDouble(args[1]);
		userInitEnable	  = Boolean.parseBoolean(args[2]);
		initRate 		  = Double.parseDouble(args[3]);
		rhoValue 		  = Double.parseDouble(args[4]);
		requestRate   	  = Double.parseDouble(args[5]);
		numAttrs 		  = Integer.parseInt(args[6]);
		numAttrsInQuery   = Integer.parseInt(args[7]);
		predicateLength   = Double.parseDouble(args[8]);
		getEnabled		  = Boolean.getBoolean(args[9]);
		
		
		System.out.println("Search and update and get client started getEnabled "+getEnabled);
		//guidPrefix = guidPrefix+myID;
		
		gnsClient  = new GNSClient();
		gnsClient = gnsClient.setForcedTimeout(5000);
		gnsClient = gnsClient.setNumRetriesUponTimeout(5);
		
		taskES = Executors.newFixedThreadPool(1);
		
		if( userInitEnable )
		{
			long start 	= System.currentTimeMillis();
			// just guid creation
			new UserInitializationClass(false).initializaRateControlledRequestSender();
			long end 	= System.currentTimeMillis();
			System.out.println(numUsers+" initialization guid creation complete "+(end-start));
			
			
			start 	= System.currentTimeMillis();
			// just guid creation
			new UserInitializationClass(true).initializaRateControlledRequestSender();
			end 	= System.currentTimeMillis();
			System.out.println(numUsers+" initialization value update complete "+(end-start));
		}
		
		BothSearchAndUpdate bothSearchAndUpdate = new BothSearchAndUpdate();
		new Thread(bothSearchAndUpdate).start();
		
		
		bothSearchAndUpdate.waitForThreadFinish();
		double avgUpdateLatency = bothSearchAndUpdate.getAverageUpdateLatency();
		double avgSearchLatency = bothSearchAndUpdate.getAverageSearchLatency();
		long numUpdates = bothSearchAndUpdate.getNumUpdatesRecvd();
		long numSearches = bothSearchAndUpdate.getNumSearchesRecvd();
		System.out.println("avgUpdateLatency "+avgUpdateLatency
					+ " avgSearchLatency "+avgSearchLatency
					+ " numUpdates "+numUpdates
					+ " numSearches "+numSearches
					+ " avg predicate length "
					+ bothSearchAndUpdate.getAvgPredLength());
		
		System.exit(0);
	}
}