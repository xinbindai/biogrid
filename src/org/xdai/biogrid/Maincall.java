package org.xdai.biogrid;

import gnu.getopt.Getopt;

public class Maincall {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitvalue = 0;
		String prompt = "Usage: biogrid [-s value] [-t value] [-d] [-p value] [-f value] [-c fasta|line] [-m value] [-o value] [-n value] CommandString\n\n"
				+ "[-s syncdir1:syncdir2], directories for sync, separated by ':'\n"
				+ "[-a],             allow sync single file using -s option; in this case, syndir1, syncdir2 can be file full path name\n"
				+ "[-e],             exitOnFailedSlice, if any computing slice fails, whole task will immediately fail with exit code -2\n"
				+ "[-g],             group mode, the first line of each slice should be like '#groupID=1234' (the number range: from 0 to slicenumber-1)\n"
				+ "[-t tmpworkdir],  tmp working directory, default is /tmp/biogrid/xxxxxxx_xxxx\n"
				+ "[-T 3000],        milliseconds for SSH command to timeout, default=3000. A value <=0 may cause application to wait forever. The SSH commands inlcudes: 1) precheck # of cpus, 2) mkdir tmp dir and 3) rsync data\n"
				+ "[-c fasta|line],  chopping mode, how to slice input file, default is fasta(by '> 'or '#'), line mode will skip blank line.\n"
				+ "[-d],             delete the tmp working folder after finishing if specify it\n"
				+ "[-p 1],           position of slice parameter in command string, start from 1, default is 1\n"
				+ "[-f 20],          factor of slice, default=20\n"
				+ "[-u 50],          uplimit of running time (times of average running time of each node), default value = 50\n"
				+ "[-m 1],           minimum slice size (in # of sequences),default=1\n"
				+ "[-i 5],           max allowed failed-times for each slice, default=3, the slice will be skipped if exceed it.\n"
				+ "[-o filename],    output file name. if omitted, output file will be slicingfile.out\n"
				+ "[-O],             print STDOUT and STDERR instead of print to file specified by -o\n"
				+ "[-n filename],    node filename. if the option is omitted, biogrid will only run on localhost\n\n"
				+ "[-P options],     string, ssh client options. such as '-i rsakeyfle -p 2222'\n\n"
				+ "[-r 1.0],         node factor, a float number larger than 0; It adjust final computing thread number for each node. default value = 1.0\n\n"
                + "[-S sr],          Sort slice by sr: larger slice goes first(default), n: small slice number goes first\n"
				+ "if CommandString is empty and -s & -n are specified, the biogrid will just synchronize with node computers\n";

		if (args.length == 0) {
			System.err.println(prompt);
			return;
		}

		String uniquename = java.util.UUID.randomUUID().toString(); //Tools.getUniqueName();
		String syncdirs = "";

		String tmpworkdir = "/tmp/biogrid/" + uniquename;
		String[] cmdlist={"id", "-u", System.getProperty("user.name")};
		ProcessRet pr = RunProcess.runcmd(cmdlist);
		if(pr.retcode==Status.FINISHED) tmpworkdir = "/tmp/biogrid_" + pr.stdout.trim() + "/" + uniquename;

		int chopmode = SliceManager.FASTA;
		int slicingPosParamter = 1;
		int factorofslice = 20;
		int uplimitrunningtime = 50;
		int minslicesize = 1; // how many sequences in each slice, final size of
								// slice should be the times of it
		boolean deletetmpworkdir = false;
		String outputfile = "";
		String nodeconfigfn = "";
		double nodefactor=1.0;
		boolean exitOnFailedSlice=false;
		boolean printStdout = false;
		boolean allowfilesync = false;
		boolean groupmode=false;
		int maxFailedTimes=3;
        String slicesort="sr";
		String ssh_parameters = "";

		Getopt g = new Getopt("biogrid", args, "i:r:s:S:t:T:c:p:f:m:o:P:u:n:d::O::a::g::e::");
		int c;
		while ((c = g.getopt()) != -1) {
			switch (c) {
		    case 'e':
				exitOnFailedSlice = true;
				break;
			case 's':
				syncdirs = g.getOptarg();
				//syncdirs = syncdirs.replaceAll("/+$", "");
				break;
			case 'a':
				allowfilesync = true;
				break;
			case 'g':
				groupmode=true;
				break;
			case 't':
				tmpworkdir = g.getOptarg();
				tmpworkdir = tmpworkdir.replaceAll("/+$", "");
				break;
			case 'c':
				if (g.getOptarg().equals("line"))
					chopmode = SliceManager.LINE;
				break;
			case 'd':
				deletetmpworkdir = true;
				break;
			case 'O':
				printStdout = true;
				break;
			case 'p':
				try {
					slicingPosParamter = Integer.parseInt(g.getOptarg());
				} catch (Exception ex) {
					System.err.println("Invalid [-p] slicing position parameters, should be integer format(>=1)");
					return;
				}
				if (slicingPosParamter < 1)
					slicingPosParamter = 1;
				break;
		    case 'T':
			    //milliseconds for SSH command timeout
				try {
					RunProcess.MAXTIMEINMILLISECONDS = Long.parseLong(g.getOptarg());
				} catch (Exception ex) {
					System.err.println("Invalid [-T] milliseconds for SSH command timeout, should be long integer format(default value: 3000). A value <=0 may cause application to wait forever.");
					return;
				}
				break;
			case 'i':
				try {
					maxFailedTimes = Integer.parseInt(g.getOptarg());
				} catch (Exception ex) {
					System.err.println("Invalid [-p] slicing position parameters, should be integer format.");
					return;
				}
				if (slicingPosParamter < 1)
					slicingPosParamter = 1;
				break;
			case 'f':
				try {
					factorofslice = Integer.parseInt(g.getOptarg());
				} catch (Exception ex) {
					System.err.println("Invalid [-f] slice-splitting factor, should be in integer format");
					return;
				}
				break;
			case 'r':
				try {
					nodefactor = Double.parseDouble(g.getOptarg());
					if(nodefactor<0) throw new Exception("");
				} catch (Exception ex) {
					System.err.println("Invalid [-r] node factor, should be a decimal larger than 0");
					return;
				}
				break;
			case 'u':
				try {
					uplimitrunningtime = Integer.parseInt(g.getOptarg());
					if(uplimitrunningtime<1) throw new Exception("");
				} catch (Exception ex) {
					System.err.println("Invalid [-u] uplimit of running time, should be in integer format and >= 1");
					return;
				}
				break;
			case 'm':
				try {
					minslicesize = Integer.parseInt(g.getOptarg());
				} catch (Exception ex) {
					System.err
							.println("Invalid [-m] min_slice_size parameters, should be integer format");
					return;
				}
				break;
			case 'o':
				outputfile = g.getOptarg();
				break;
    		case 'P':
				ssh_parameters = g.getOptarg();
				break;
			case 'n':
				nodeconfigfn = g.getOptarg();
				break;
			case 'S':
				slicesort = g.getOptarg();
                if(!slicesort.equals("n")) slicesort="sr";
				break;			
            case '?':
				break; // getopt() already printed an error
			default:
				System.out.print("getopt() returned " + c + "\n");
			}
		}

		String cmd = "";
		int firstpar = g.getOptind();
		if (args.length > firstpar) {
			cmd = args[firstpar];
		}
		if (cmd.equals("")&&syncdirs.equals("")) { // if cmd='' and syncdir =''. exit
			System.err.println(prompt);
			System.exit(-1);
		}

		/************************************Program********************************************************************/
		System.err.println("Working-Folder="	+ tmpworkdir);
		long time1 = System.currentTimeMillis();
		System.err.print("Initial nodes based on node config file "+nodeconfigfn+" ......");

		//check and initial nodes, threads number,sync and prepare workdir....... 
		MasterNode mn = MasterNode.MasterNodeFactory(nodeconfigfn, nodefactor, tmpworkdir, syncdirs, allowfilesync, cmd, slicingPosParamter, maxFailedTimes, ssh_parameters);
        //if no command, only sync/makeworkdir and return
		if (cmd.equals("")) {
			for(ComputingNode cn: mn.getComputingnodes()) cn.deleteWorkdir();
			return;
		}

		//prepare slices
		SliceManager sm = null;
		try {
			String inputfn = Tools.getSliceSourcefn(cmd, slicingPosParamter);  //input file which need to be sliced
			System.err.print("Slicing input file "+inputfn+" ......");
			//Tools.getMyAddress()+"_"+Tools.getUniqueName()+"_"
			sm = SliceManager.SliceManagerFactory("slice_", mn.getComputingthreads().size(), inputfn, 
					                           factorofslice, minslicesize, tmpworkdir, chopmode, groupmode, slicesort);
			System.err.println("done.\nSlice size="+sm.getSlicesize()+"   Slice total number="+sm.getSliceTotalNumber());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		// run parallel system
		System.err.println("Running......");
		ProgressMeter pm = new ProgressMeter(mn, sm);
		long time2 = System.currentTimeMillis();
		while(sm.hasReadySlice()||sm.hasRunningSlice()) { 	//System.err.print("Step 1="+System.currentTimeMillis());

		    if(exitOnFailedSlice&&sm.hasFailedSlice()){
				System.err.println("ERROR: Some computing slices failed, abort......");
				exitvalue = -2;
				break;
			}
			if (!mn.dispatch(sm)) {   //dispatching......., if not at-Least-One-Node-Is-Active-or-!disable, abort.
				System.err.println("ERROR: All computing nodes are inactive, abort......");
				exitvalue = -1;
				break;
			}
			long secs = System.currentTimeMillis()/1000;
			if(secs%10==0)   MasterNode.killStuckJobs(mn, sm, uplimitrunningtime);    //kill stuck jobs every 10 seconds
			if(secs%120==0)  MasterNode.ActivateAllComputingThreads(mn);
			if(secs%5==0)   pm.printall();
			Thread.sleep(200);
		}
		if (exitvalue != 0)	System.exit(exitvalue);
		pm.printall();
		System.err.println();

		// consolidate result
		if (printStdout) {
			sm.MergeOutputFile(System.out, System.err, null);
		} else {
			sm.MergeOutputFile(outputfile, null);
			System.err.println("All of outputs were written to     " + sm.getFinalResultOutputFile());
			System.err.println("All of error info were written to  " + sm.getFinalResultErrFile());
		}
		if (deletetmpworkdir) {
			for(ComputingNode cn: mn.getComputingnodes()){
				cn.deleteWorkdir();
			}
		}

		long time3 = System.currentTimeMillis();
		System.err.println("Total time:      " + (time3 - time1) / 1000	+ " seconds");
		System.err.println("Sync/slice time: " + (time2 - time1) / 1000	+ " seconds");
		System.err.println("Running time:    " + (time3 - time2) / 1000	+ " seconds.");
	} // end of function

} // end of class
