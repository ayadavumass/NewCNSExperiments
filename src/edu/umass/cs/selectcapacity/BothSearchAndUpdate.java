package edu.umass.cs.selectcapacity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

public class BothSearchAndUpdate extends 
					AbstractRequestSendingClass implements Runnable
{
	private final Random generalRand;
	private Random searchQueryRand;
	private final Random updateRand;
	
	private double currUserGuidNum   		= 0;
	
	private long sumResultSize				= 0;
	
	private long sumSearchLatency			= 0;
	private long sumUpdateLatency			= 0;
	
	private long numSearchesRecvd			= 0;
	private long numUpdatesRecvd			= 0;
	
	
	// we don't want to issue new search queries for the trigger exp.
	// so that the number of search queries in the experiment remains same.
	// so when number of search queries reaches threshold then we reset it to 
	// the beginning.
	//private long numberSearchesSent		= 0;
	
	private double sumPredLength			= 0;
	private long numEntries					= 0;
	
	public BothSearchAndUpdate()
	{
		super( SearchAndUpdateDriver.UPD_LOSS_TOLERANCE );
		generalRand = new Random(SearchAndUpdateDriver.myID);
		updateRand = new Random(SearchAndUpdateDriver.myID*100);
		
		searchQueryRand = new Random(SearchAndUpdateDriver.myID*200);
	}
	
	@Override
	public void run()
	{
		try
		{
			this.startExpTime();
			rateControlledRequestSender();
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void rateControlledRequestSender() throws Exception
	{
		double reqsps = SearchAndUpdateDriver.requestRate;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqsps;
		
		while( ( (System.currentTimeMillis() - expStartTime) < 
				SearchAndUpdateDriver.experimentTime ) )
		{
			for( int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendRequest(numSent);
				numSent++;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = (timeElapsed*reqsps)/1000.0;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendRequest(numSent);
				numSent++;
			}
			Thread.sleep(1000);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("Both eventual sending rate "+sendingRate);
		
		waitForFinish();
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		double avgResultSize = 0;
		if( this.numSearchesRecvd > 0 )
		{
			avgResultSize = (sumResultSize/this.numSearchesRecvd);
		}
		
		System.out.println("Both result:Goodput "+sysThrput+" average resultsize "
										+avgResultSize);
	}	
	
	private void sendRequest( long reqIdNum )
	{
		// send update
		if( generalRand.nextDouble() < SearchAndUpdateDriver.rhoValue )
		{
			sendQueryMessageWithSmallRanges(reqIdNum);
		}
		else
		{
			sendUpdate(reqIdNum);
		}
	}
	
	private void sendUpdate(long reqIdNum)
	{
		sendUpdateMessage((int)currUserGuidNum, reqIdNum);
		currUserGuidNum++;
		currUserGuidNum=((int)currUserGuidNum)%SearchAndUpdateDriver.numUsers;
	}
	
	private void sendQueryMessageWithSmallRanges(long reqIdNum)
	{
		//String query = "$and:[(\"~a0\":($gt:0, $lt:100)),(\"~a1\":($gt:0, $lt:100))]";
		
		HashMap<String, Boolean> distinctAttrMap 
			= pickDistinctAttrs( SearchAndUpdateDriver.numAttrsInQuery, 
					SearchAndUpdateDriver.numAttrs, searchQueryRand );
		
		Iterator<String> attrIter = distinctAttrMap.keySet().iterator();
		
		String searchQuery = "$and:[";
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			double attrMin = SearchAndUpdateDriver.ATTR_MIN
					+searchQueryRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
									- SearchAndUpdateDriver.ATTR_MIN);
			
			// querying 10 % of domain
			double predLength 
				= (SearchAndUpdateDriver.predicateLength
						*(SearchAndUpdateDriver.ATTR_MAX - SearchAndUpdateDriver.ATTR_MIN)) ;
			
			double attrMax = attrMin + predLength;
			
			if( attrMax > SearchAndUpdateDriver.ATTR_MAX )
			{
				attrMax = SearchAndUpdateDriver.ATTR_MAX;
			}
			
			//attrMin = SearchAndUpdateDriver.ATTR_MIN;
			//attrMax = SearchAndUpdateDriver.ATTR_MAX;
			
			sumPredLength = sumPredLength + 
					((attrMax-attrMin)/(SearchAndUpdateDriver.ATTR_MAX-SearchAndUpdateDriver.ATTR_MIN));
			this.numEntries = this.numEntries + 1;
			
			
			String predicate = "(\"~"+attrName+"\":($gt:"+attrMin+", $lt:"+attrMax+"))";
			
			// last so no AND
			if( !attrIter.hasNext() )
			{
				searchQuery = searchQuery +predicate+"]";
			}
			else
			{
				searchQuery = searchQuery +predicate+",";
			}
		}
		
		try 
		{
			SearchAndUpdateDriver.gnsClient.execute
				(GNSCommand.selectQuery(searchQuery), new SearchCallBack(this));
		} catch (ClientException | IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	
	private HashMap<String, Boolean> pickDistinctAttrs( int numAttrsToPick, 
			int totalAttrs, Random randGen )
	{
		HashMap<String, Boolean> hashMap = new HashMap<String, Boolean>();
		int currAttrNum = 0;
		while(hashMap.size() != numAttrsToPick)
		{
			if(SearchAndUpdateDriver.numAttrs == SearchAndUpdateDriver.numAttrsInQuery)
			{
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
				currAttrNum++;
			}
			else
			{
				currAttrNum = randGen.nextInt(SearchAndUpdateDriver.numAttrs);
				String attrName = "attr"+currAttrNum;
				hashMap.put(attrName, true);
			}
		}
		return hashMap;
	}
	
	
	private void sendUpdateMessage( int currUserGuidNum, long reqIdNum )
	{
		String alias = SearchAndUpdateDriver.ALIAS_PREFIX+SearchAndUpdateDriver.myID
				+currUserGuidNum+SearchAndUpdateDriver.ALIAS_SUFFIX;
		
		GuidEntry guidEntry = GuidUtils.getGUIDKeys(alias);
		
		int randomAttrNum = updateRand.nextInt(SearchAndUpdateDriver.numAttrs);
		double randVal = SearchAndUpdateDriver.ATTR_MIN 
				+updateRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
						- SearchAndUpdateDriver.ATTR_MIN);
		
		
		JSONObject attrValJSON = new JSONObject();
		try
		{
			attrValJSON.put(SearchAndUpdateDriver.ATTR_PREFIX+randomAttrNum, randVal);
		} 
		catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		try 
		{
			SearchAndUpdateDriver.gnsClient.execute
					(GNSCommand.update(guidEntry, attrValJSON), new UpdateCallBack(this));
		} 
		catch (ClientException | IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public double getAverageUpdateLatency()
	{
		return (this.numUpdatesRecvd>0)?sumUpdateLatency/this.numUpdatesRecvd:0;
	}
	
	public double getAverageSearchLatency()
	{
		return (this.numSearchesRecvd>0)?sumSearchLatency/this.numSearchesRecvd:0;
	}
	
	public long getNumUpdatesRecvd()
	{	
		return this.numUpdatesRecvd;
	}
	
	public long getNumSearchesRecvd()
	{
		return this.numSearchesRecvd;
	}
	
	public double getAvgPredLength()
	{
		return this.sumPredLength/this.numEntries;
	}
	
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.numUpdatesRecvd++;
			
			this.sumUpdateLatency = this.sumUpdateLatency + timeTaken;
			
			if((numRecvd % 100) == 0)
			{
				System.out.println("Update recvd current stats total sent="+numSent
						+" total recvd="+numRecvd
						+" update recvd="+this.numUpdatesRecvd
						+" search recvd="+this.numSearchesRecvd);
			}
			
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}
	
	@Override
	public void incrementSearchNumRecvd(int resultSize, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			this.numSearchesRecvd++;
			sumResultSize = sumResultSize + resultSize;
			
			//if((numRecvd % 100) == 0)
			{
				System.out.println("Search recvd current stats total sent="+numSent
						+" total recvd="+numRecvd
						+" update recvd="+this.numUpdatesRecvd
						+" search recvd="+this.numSearchesRecvd);
			}
			
			this.sumSearchLatency = this.sumSearchLatency + timeTaken;
			if( checkForCompletionWithLossTolerance(numSent, numRecvd) )
			{
				waitLock.notify();
			}
		}
	}
}