package edu.umass.cs.selectcapacity;


import java.io.IOException;
import java.util.Random;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;


/**
 * This class creates GUIDs for the first time in the GNS based CNS
 * @author ayadav
 */
public class UserInitializationClass extends AbstractRequestSendingClass
{
	// different random generator for each variable, as using one for all of them
	// doesn't give uniform properties.
	private final boolean performValueUpdate;
	private final Random initRand;
	
	public UserInitializationClass(boolean performValueUpdate)
	{
		super( SearchAndUpdateDriver.INSERT_LOSS_TOLERANCE );
		this.performValueUpdate = performValueUpdate;
		initRand = new Random(SearchAndUpdateDriver.myID*100);
	}
	

	private void sendAInitMessage(int guidNum) throws Exception
	{
		String alias = SearchAndUpdateDriver.ALIAS_PREFIX+
						SearchAndUpdateDriver.myID+guidNum+
						SearchAndUpdateDriver.ALIAS_SUFFIX;
		
		if(!performValueUpdate)
		{
			System.out.println("Creating account GUID "+alias);
			SearchAndUpdateDriver.gnsClient.execute
				(GNSCommand.createAccount(alias), new InitCallBack(this));
		}
		else
		{
			initializeAttrValues( alias, guidNum );
		}
	}
	
	private void initializeAttrValues( String alias, int guidNum )
	{	
		GuidEntry guidEntry = GuidUtils.getGUIDKeys(alias);
		SearchAndUpdateDriver.guidEntryArray[guidNum] = guidEntry;
		
		
		JSONObject attrValJSON = new JSONObject();
		for(int i=0; i<SearchAndUpdateDriver.numAttrs; i++)
		{
			String attrName = SearchAndUpdateDriver.ATTR_PREFIX+i;
			double randVal = SearchAndUpdateDriver.ATTR_MIN 
					+initRand.nextDouble()*(SearchAndUpdateDriver.ATTR_MAX 
							- SearchAndUpdateDriver.ATTR_MIN);
			
			try
			{
				attrValJSON.put(attrName, randVal);
			} 
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		try 
		{
			SearchAndUpdateDriver.gnsClient.execute
					(GNSCommand.update(guidEntry, attrValJSON), new UpdateCallBack(this));
//			UpdateCallBack updcb = new UpdateCallBack(this);
//			CommandPacket cmdp = SearchAndUpdateDriver.gnsClient.execute
//			(GNSCommand.update(guidEntry, attrValJSON) );
//			updcb.processResponse(cmdp);
		}
		catch (ClientException | IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public void initializaRateControlledRequestSender() throws Exception
	{	
		this.startExpTime();
		double reqspms = SearchAndUpdateDriver.initRate/1000.0;
		long currTime = 0;
		
		// sleep for 100ms
		double numberShouldBeSentPerSleep = reqspms*100.0;
		
		double totalNumUsersSent = 0;
		
		while(  totalNumUsersSent < SearchAndUpdateDriver.numUsers  )
		{
			for(int i=0; i<numberShouldBeSentPerSleep; i++ )
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
			}
			currTime = System.currentTimeMillis();
			
			double timeElapsed = ((currTime- expStartTime)*1.0);
			double numberShouldBeSentByNow = timeElapsed*reqspms;
			double needsToBeSentBeforeSleep = numberShouldBeSentByNow - numSent;
			if(needsToBeSentBeforeSleep > 0)
			{
				needsToBeSentBeforeSleep = Math.ceil(needsToBeSentBeforeSleep);
			}
			
			for(int i=0;i<needsToBeSentBeforeSleep;i++)
			{
				sendAInitMessage((int)totalNumUsersSent);
				totalNumUsersSent++;
				numSent++;
				assert(numSent == totalNumUsersSent);
				if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
				{
					break;
				}
			}
			if(totalNumUsersSent >= SearchAndUpdateDriver.numUsers)
			{
				break;
			}
			Thread.sleep(100);
		}
		
		long endTime = System.currentTimeMillis();
		double timeInSec = ((double)(endTime - expStartTime))/1000.0;
		double sendingRate = (numSent * 1.0)/(timeInSec);
		System.out.println("UserInit performValueUpdate "+this.performValueUpdate+" eventual sending rate "+sendingRate);
		
		waitForFinish();
		
		double endTimeReplyRecvd = System.currentTimeMillis();
		double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
		
		System.out.println("UserInit performValueUpdate "+performValueUpdate
							+" result:Goodput "+sysThrput);	
	}
	
	@Override
	public void incrementUpdateNumRecvd(String userGUID, long timeTaken)
	{
		synchronized(waitLock)
		{
			numRecvd++;
			if(!this.performValueUpdate)
			{
				System.out.println("Account GUID "+userGUID+" created time taken "+timeTaken+
					" numSent "+numSent+" numRecvd "+numRecvd);
			}
			else
			{
				System.out.println("AttrValue initialized for "+userGUID
						+" time taken "+timeTaken+
						" numSent "+numSent+" numRecvd "+numRecvd);
			}
			if(checkForCompletionWithLossTolerance(numSent, numRecvd))
			{
				waitLock.notify();
			}
		}
	}

	@Override
	public void incrementSearchNumRecvd(int resultInfo, long timeTaken) 
	{
	}


	@Override
	public void incrementGetNumRecvd(JSONObject resultJSON, long timeTaken) 
	{	
	}
}