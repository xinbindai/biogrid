package org.xdai.biogrid;

import java.util.TimerTask;

public class ProgressMeter extends TimerTask {

	private int lastprintedratio = 0;
	private boolean hasPrintMinorRatio = false;
	private boolean hasPrintZeroRatio = false;
	private MasterNode mn;
	private SliceManager sm;

	public ProgressMeter(MasterNode mn, SliceManager sm) {
		this.mn = mn;
		this.sm = sm;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		printall();
	}

	public void printall() {
		double curratio = sm.getProgress()	* 100;

		if (curratio == 0 && !hasPrintZeroRatio) {
			printprogress(curratio);
			hasPrintZeroRatio = true;
		} else if(curratio > 0.001 && curratio < 0.99 && !hasPrintMinorRatio) {
			printprogress(curratio);
			hasPrintMinorRatio = true;
		} else if(curratio >= 1 && ((int) curratio < 100	&& (int) curratio > (this.lastprintedratio + 2) 
		       || (int) curratio == 100 && (int) curratio > this.lastprintedratio)) {
			printprogress(curratio);
			this.lastprintedratio = (int) curratio;
		}
	}

	public void printprogress(double ratio) {
		String nodestr = mn.getRunningComputingThreadNum() + "/" + mn.getComputingthreads().size();
		if (ratio >= 1) {
			System.err.printf("Finished: %5d %1s;\trunning/total nodes: %s\n",	(int)ratio, "%", nodestr);
		} else {
			System.err.printf("Finished: %5.3f %1s;\trunning/total nodes: %s\n", ratio,	"%", nodestr);
		}
	}
}
