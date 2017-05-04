package edu.umass.cs.selectcapacity;

import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gigapaxos.interfaces.Callback;

/**
 * Init call back for GUID creation.
 * @author ayadav
 *
 */
public class UpdateCallBack implements Callback<CommandPacket, CommandPacket>
{	
	private final AbstractRequestSendingClass sendingObj;
	private final long startTime;
	
	public UpdateCallBack(AbstractRequestSendingClass sendingObj)
	{
		this.sendingObj = sendingObj;
		startTime = System.currentTimeMillis();
	}
	
	@Override
	public CommandPacket processResponse(CommandPacket request) 
	{
		CommandPacket cmd = (CommandPacket) request;
		sendingObj.incrementUpdateNumRecvd(cmd.getServiceName(), 
				(System.currentTimeMillis()-startTime));
		return cmd;
	}
}