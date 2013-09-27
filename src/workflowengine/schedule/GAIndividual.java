/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.Random;

/**
 *
 * @author Dew
 */
public class GAIndividual extends Schedule
{
    private Random r = new Random();

    public GAIndividual(SchedulerSettings settings)
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
        int randomTask = r.nextInt(settings.getTotalTasks());
        int randomServer = r.nextInt(settings.getTotalWorkers());
        setWorkerForTask(randomTask, settings.getWorker(randomServer));
    }

    GAIndividual[] crossover(GAIndividual s2)
    {
        int crossoverPoint = r.nextInt(settings.getTotalTasks());
        GAIndividual[] child = new GAIndividual[2];
        child[0] = new GAIndividual(settings);
        child[1] = new GAIndividual(settings);
        for (int i = 0; i < crossoverPoint; i++)
        {
            child[0].setWorkerForTask(i, this.getWorkerForTask(i));
            child[1].setWorkerForTask(i, s2.getWorkerForTask(i));
        }
        for (int i = crossoverPoint; i < settings.getTotalTasks(); i++)
        {
            child[0].setWorkerForTask(i, s2.getWorkerForTask(i));
            child[1].setWorkerForTask(i, this.getWorkerForTask(i));
        }
        return child;
    }
}
