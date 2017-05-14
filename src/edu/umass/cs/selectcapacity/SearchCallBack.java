package edu.umass.cs.selectcapacity;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;

/**
 * Init call back for GUID creation.
 * @author ayadav
 *
 */
public class SearchCallBack implements Callback<CommandPacket, CommandPacket>
{	
	private final AbstractRequestSendingClass sendingObj;
	private final long startTime;
	
	public SearchCallBack(AbstractRequestSendingClass sendingObj)
	{
		this.sendingObj = sendingObj;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public CommandPacket processResponse(CommandPacket request) 
	{
		CommandPacket cmd = (CommandPacket) request;
		try
		{
			System.out.println("Response cmd "+cmd );
			sendingObj.incrementSearchNumRecvd(cmd.getResultList().size(), 
					(System.currentTimeMillis()-startTime));
		} catch (ClientException e) 
		{
			e.printStackTrace();
		}
		return cmd;
	}
}