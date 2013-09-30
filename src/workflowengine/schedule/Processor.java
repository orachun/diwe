/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

/**
 *
 * @author orachun
 */
public class Processor
{

	/**
	 * The start time of the current scheduled job.
	 */
	private double mStartTime;
	/**
	 * The end time of the current scheduled job.
	 */
	private double mEndTime;

	/**
	 * The default constructor.
	 */
	public Processor()
	{
		mStartTime = 0;
		mEndTime = 0;
	}

	/**
	 * Returns the earliest time the processor is available for scheduling a
	 * job. It is non insertion based scheduling policy.
	 *
	 * @param start the time at which to start the search.
	 *
	 * @return double
	 */
	public double getAvailableTime(double start)
	{
		return (mEndTime > start) ? mEndTime : start;
	}

	/**
	 * Schedules a job on to a processor.
	 *
	 * @param start the start time of the job.
	 * @param end the end time for the job
	 */
	public void scheduleJob(double start, double end)
	{
		mStartTime = start;
		mEndTime = end;
	}
}