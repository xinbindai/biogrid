package org.xdai.biogrid;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

public class Tools {
	static int mypid = 0;
	static String prefix = null;
	static String[] myips = null;

	static String getFileContent(String filename) {
		StringBuffer sb = new StringBuffer();
		try {
			InputStream in = new FileInputStream(new File(filename));
			byte[] buf = new byte[1024*64];
			for (;;) {
				int len = in.read(buf);
				if (len < 0) break;
				sb.append(new String(buf, 0, len));
			}
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}


	static String getSliceSourcefn(String cmd, int slicepos) throws Exception {
		String cmdlist[] = cmd.split("[ \t]+");
		if (cmdlist==null || slicepos >= cmdlist.length || slicepos<0)	
			throw new Exception("Failed to get input filename from command line. Command line:" + cmd + "; sliceposparam:" + slicepos);
		return cmdlist[slicepos];
	}

	static int getLocalProcessors() {
		return Runtime.getRuntime().availableProcessors();
	} // end of function
	
	static String getSSHoption(){
		String opts=" -q -o PasswordAuthentication=no -o StrictHostKeyChecking=no -o ConnectTimeout=2 ";
		return opts;
	}
	
	static int getRemoteProcessors(String hostname) {
		String cmd = "ssh" + getSSHoption() + hostname + " nproc";
		String[] tmpcmd = { "bash", "-c", cmd };
		ProcessRet ret=RunProcess.runcmd(tmpcmd, 3000);
		String pid = ret.stdout.replaceAll("[^0-9]", "");
		int nprocs=0;
		try{   nprocs=Integer.parseInt(pid); }
		catch(Exception e) { }
		return nprocs; 
	} // end of function

	static int getMyPID() {
		if (mypid > 0)
			return mypid;
		byte[] bo = new byte[100];
		String[] cmd = { "bash", "-c", "echo $PPID" };
		Process p = null;
		try {
			p = Runtime.getRuntime().exec(cmd);
			p.getInputStream().read(bo);
		} catch (IOException e) {
			e.printStackTrace();
		}
		String pid = new String(bo);
		pid = pid.replaceAll("[^0-9]", "");
		return Integer.parseInt(pid);
	} // end of function

	public static String LOCALHOSTIP="127.0.0.1"; 
	
	static ArrayList<Map.Entry<String, Integer>> readNodeThreads(String filename, double cpufactor) {
		ArrayList<Map.Entry<String, Integer>> v = new ArrayList<Map.Entry<String, Integer>>();
		if (filename == null || filename.equals("")) {
			AbstractMap.SimpleEntry<String, Integer> as = new AbstractMap.SimpleEntry<String, Integer>(Tools.LOCALHOSTIP, Tools.getLocalProcessors());
			v.add(as);
			int finalcpus=(int)(v.size()*cpufactor);
			System.err.println("  Automatically add " + finalcpus + " local node(s)");
		} else {
			try {
				BufferedReader in = new BufferedReader(new FileReader(filename));
				String str;
				int totalcpus=0;
				while ((str = in.readLine()) != null) {
					if (str.matches("^\\s*$") || str.matches("^\\s*#.*$"))	continue; // skip empty and comment line
					str = str.replaceAll("^\\s+|\\s+$", ""); // trim
					String[] res = str.split("[\t ]+"); // split

					if (res.length <= 1) continue; // if there is no two cols, skip.
				    if(Tools.isLocalHost(res[0])) res[0] =Tools.LOCALHOSTIP;
					
					double paracpus = 0.0; // parameter for # of CPUs in config file, effective format: 0 / 12 / 0.25
					try {
						paracpus = Double.parseDouble(res[1]);
					} catch (Exception ex) {
						System.err.println("Invalid CPU number for " + res[0]+", node config is: "+str);
						continue;
					}

					int finalcpus = 0; // the final cpu number based on current situation and config file.
					if (paracpus >= 1.0) {
						finalcpus = (int) paracpus;
					}else{
					    int detectedcpus = 0; // real # of processor in computing node
					    if(Tools.isLocalHost(res[0])) {
						  detectedcpus = Tools.getLocalProcessors();
					    } else {
						  detectedcpus = Tools.getRemoteProcessors(res[0]);
					    } 

					    if (paracpus <= 0.0) {
						  finalcpus = detectedcpus;
					    }else {
						  finalcpus = (int) (paracpus * detectedcpus + 0.5);			  // if(cpus==0) cpus=1;
					    }
					}
					finalcpus=(int)(finalcpus*cpufactor);
					if(finalcpus>0){
						AbstractMap.SimpleEntry<String, Integer> as = new AbstractMap.SimpleEntry<String, Integer>(res[0], finalcpus);
						v.add(as);
						totalcpus=totalcpus+finalcpus;
					}
					if (v.size() > 1) System.err.print(","); // don't print comma for the first node computer
					System.err.print("  " + finalcpus + " nodes " + res[0]);
				}
				System.err.println();
				System.err.println("TOTAL: " + totalcpus + " nodes");
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
        return v;
	} // end of function

	static String getMyAddress() {
		String cmd[] = { "bash", "-c", "hostname" };
		ProcessRet ret = RunProcess.runcmd(cmd);
		return ret.stdout.replaceAll("\\s", "");
	}

	static String getUniqueName() {
		return System.currentTimeMillis() + "_" + Tools.getMyPID();
	}

/*	
	static boolean isLocalHost(String hostname) {
		String purehostname=hostname.replaceAll("^[^@]*@", "");
		InetAddress ia;
		try {
			ia = InetAddress.getByName(purehostname);
			String ip = ia.getHostAddress();
			if (ip.startsWith("127."))	return true;
			if (myips == null) {
				String cmd[] = { "bash", "-c", "ifconfig|grep 'inet addr:'|awk '{print $2}'|sed -re 's/addr://'" };
				ProcessRet ret = RunProcess.runcmd(cmd);
				myips = ret.stdout.split("\n");
			}
			for (int i = 0; i < myips.length; i++) {
				if (ip.equals(myips[i])) return true;
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return false;
	}
*/

	static boolean isLocalHost(String hostname) {
		String purehostname=hostname.replaceAll("^[^@]*@", "");
		boolean localhost=false;
		try{
		    localhost = isLocalHost2(purehostname);
		}catch(Exception e ){}
		return localhost;
	}	
	
	 public static boolean isLocalHost2(String name) throws UnknownHostException, SocketException {
	        InetAddress[] addresses = InetAddress.getAllByName(name);
	        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
	        while (networkInterfaces.hasMoreElements()) {
	            NetworkInterface networkInterface = (NetworkInterface) networkInterfaces.nextElement();
	            if(!networkInterface.isUp())
	                continue;
	            Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
	            while (inetAddresses.hasMoreElements()) {
	                InetAddress inetAddress = (InetAddress) inetAddresses.nextElement();
	                if(addressMatch(addresses, inetAddress))
	                    return true ;
	            }

	        }
	        return false;
	    }

	    private static boolean addressMatch(InetAddress[] addresses, InetAddress inetAddress) {
	        for (InetAddress address : addresses) {
	            if(addrMatch(address, inetAddress))
	                return true;
	        }
	        return false;
	    }
	    
	    private static boolean addrMatch(InetAddress address, InetAddress inetAddress) {
	        byte[] address2 = inetAddress.getAddress();
	        byte[] address3 = address.getAddress();
	        for(int i = 0; i < 4; i++)
	            if(address2[i] != address3[i])
	                return false ;
	        return true;
	    }	
}
