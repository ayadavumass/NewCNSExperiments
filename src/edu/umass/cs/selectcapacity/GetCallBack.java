package edu.umass.cs.selectcapacity;

import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;

/**
 * Get callback for GUID creation.
 * @author ayadav
 *
 */
public class GetCallBack implements Callback<CommandPacket, CommandPacket>
{	
	private final AbstractRequestSendingClass sendingObj;
	private final long startTime;
	
	public GetCallBack(AbstractRequestSendingClass sendingObj)
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
			sendingObj.incrementGetNumRecvd(cmd.getResultJSONObject(), 
					(System.currentTimeMillis()-startTime));
		} catch (ClientException e) 
		{
			e.printStackTrace();
		}
		return cmd;
	}
}