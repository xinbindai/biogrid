package org.xdai.biogrid;

import java.util.ArrayList;

public class SyncDirs {
	
	private String[] dirs;
	private ArrayList<ComputingNode> cns;

	public SyncDirs(String strdirs, ArrayList<ComputingNode> cns, boolean allowfilesync) throws Exception {
		dirs = strdirs.split(":+");
		for (int i = 0; i < dirs.length; i++) {
			if(!allowfilesync) dirs[i] = dirs[i].replaceAll("/+$", "") + "/";
			if (!dirs[i].startsWith("/")) {
				String tmp = " '" + dirs[i] + "' is not an absolute path (doesn't start with '/'), abort! ";
				System.err.println("\n" + tmp);
				throw new Exception(tmp);
			}
		}
		this.cns = cns;
	}

	public void dosync() {

        ParallelRunProcess prp = new ParallelRunProcess();
        
		for (int i = 0; i < dirs.length; i++) {
			System.err.print("    For " + dirs[i] + ": ");
			for (int j = 0; j < cns.size(); j++) {
				ComputingNode cn = cns.get(j);
				if(cn.isLocalhost())  continue;
				if(cn.disable)  continue;
				System.err.print(" " + cn.getHostname() + " ");

				String makesyncdircmd = "tmpname="	+ dirs[i]
						+ ";if [[ -e $tmpname && ! -d $tmpname ]]; then rm -f $tmpname;fi;if [[ ! -e $tmpname ]]; then mkdir -p $tmpname;fi; if [[ -e $tmpname && -d $tmpname ]]; then echo \"exists\";fi";
				String tmpcmd[] = { "ssh", cn.getHostname(), makesyncdircmd };
				ProcessRet retres=RunProcess.runcmd(tmpcmd, 3000);
				if(retres.retcode==Status.FAILED || retres.retcode==Status.FAILEDTORUN) {
					   cn.disable=true;
					   System.err.println();
					   System.err.println("Failed to make sync-working folder on computing node "+cn.getHostname()+", disabled it! Below is error message for debug:");
					   System.err.println(retres.stderr);
					   System.err.flush();
					   continue;
				}
                // -a == -rlptgoD (no -H,-A,-X)
				String tmpcmd2[] = { "rsync", "-rlpz", "--delete", "--timeout", "1800", "--exclude", "tmp", "--exclude", "/usr", dirs[i],	cn.getHostname() + ":" + dirs[i] };
				RunProcess myr = new RunProcess(tmpcmd2);
				myr.setRelobj(cn);
				prp.add(myr);
			}
			System.err.print(".  ");
		}
		
		for(RunProcess tmp: prp.getFailedRunProcesses()){
			ComputingNode tmpcn = (ComputingNode)tmp.getRelobj();
			if(tmpcn!=null) {
			   tmpcn.disable=true;
			   System.err.println();
			   System.err.println("Failed to sync with computing node "+tmpcn.getHostname()+", disabled it! Below is error message for debug:");
			   System.err.println(tmp.retrieveRet().stderr);
			   System.err.flush();
			}
		}

	} // end of function
}
