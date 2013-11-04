/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;

/**
 *
 * @author Dew
 */
public class GAIndividual extends Schedule
{
    private Random r = new Random();

    public GAIndividual(SchedulingSettings settings)
    {
        super(settings);
    }

    public GAIndividual(Schedule s)
    {
        super(s);
    }

    public void random(int resourceCount)
    {
        super.random();
    }


    void mutation()
    {
		String[] workers = (String[])settings.getObjectParam("workers");
		String[] tasks = (String[])settings.getObjectParam("tasks");
        int randomTask = r.nextInt(tasks.length);
        int randomServer = r.nextInt(workers.length);
        setWorkerForTask(tasks[randomTask], workers[randomServer]);
    }

    GAIndividual[] crossover(GAIndividual s2)
    {
		String[] tasks = (String[])settings.getObjectParam("tasks");
        
		int crossoverPoint = r.nextInt(settings.getTotalTasks());
        GAIndividual[] child = new GAIndividual[2];
        child[0] = new GAIndividual(settings);
        child[1] = new GAIndividual(settings);
		
        for (int i = 0; i < crossoverPoint; i++)
        {
            child[0].setWorkerForTask(tasks[i], this.getWorkerForTask(tasks[i]));
            child[1].setWorkerForTask(tasks[i], s2.getWorkerForTask(tasks[i]));
        }
        for (int i = crossoverPoint; i < settings.getTotalTasks(); i++)
        {
            child[0].setWorkerForTask(tasks[i], s2.getWorkerForTask(tasks[i]));
            child[1].setWorkerForTask(tasks[i], this.getWorkerForTask(tasks[i]));
        }
        return child;
    }
}
