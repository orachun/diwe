/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

/**
 *
 * @author orachun
 */
public class Site
{

	/**
	 * The number of processors making up a site.
	 */
	private int mNumProcessors;
	/**
	 * A list of processors making up the site.
	 */
	private List mProcessors;
	/**
	 * The index to the processor that is to be used for scheduling a job.
	 */
	private int mCurrentProcessorIndex;
	/**
	 * The logical name assigned to the site.
	 */
	private String uri;


	/**
	 * The overloaded constructor.
	 *
	 * @param uri the name to be assigned to the site.
	 * @param num the number of processors.
	 */
	public Site(String uri, int num)
	{
		this.uri = uri;
		mNumProcessors = num;
		mCurrentProcessorIndex = -1;
		mProcessors = new LinkedList();
	}

	
	/**
	 * Returns the earliest time the site is available for scheduling a job. It
	 * is non insertion based scheduling policy.
	 *
	 * @param start the time at which to start the search.
	 *
	 * @return double
	 */
	public double getAvailableTime(double start)
	{
		int num = 0;

		//each processor is checked for start of list
		double result = Double.MAX_VALUE;
		double current;
		ListIterator it;
		for (it = mProcessors.listIterator(); it.hasNext(); num++)
		{
			Processor p = (Processor) it.next();
			current = p.getAvailableTime(start);
			if (current < result)
			{
				//tentatively schedule a job on the processor
				result = current;
				mCurrentProcessorIndex = num;
			}
		}

		if (result > start && num < mNumProcessors)
		{
			//tentatively schedule a job to an unused processor as yet.
			result = start;
			mCurrentProcessorIndex = num++;
			//if using a normal iterator
			//could use addLast() method
			it.add(new Processor());

		}

		//sanity check
		if (result == Double.MAX_VALUE)
		{
			throw new RuntimeException("Unable to scheduled to site");
		}

		return result;
	}

	/**
	 * Schedules a job to the site.
	 *
	 * @param start the start time of the job.
	 * @param end the end time for the job
	 */
	public void scheduleJob(double start, double end)
	{
		//sanity check
		if (mCurrentProcessorIndex == -1)
		{
			throw new RuntimeException("Invalid State. The job needs to be tentatively scheduled first!");
		}

		Processor p = (Processor) mProcessors.get(mCurrentProcessorIndex);
		p.scheduleJob(start, end);

		//reset the index
		mCurrentProcessorIndex = -1;
	}

	/**
	 * Returns the name of the site.
	 *
	 * @return name of the site.
	 */
	public String getName()
	{
		return uri;
	}

	/**
	 * Returns the number of available processors.
	 *
	 * @return number of available processors.
	 */
	public int getAvailableProcessors()
	{
		return this.mNumProcessors;
	}
}
