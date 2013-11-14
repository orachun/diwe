/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author orachun
 */
public class ModifiedFixedThreadPool implements ExecutorService
{
	private ExecutorService pool;
	private Map<Future, Future> futures = new ConcurrentHashMap<>();

	public ModifiedFixedThreadPool(int threads)
	{
		pool = Executors.newFixedThreadPool(threads);
	}
	
	public void waitUntilAllTaskFinish() throws ExecutionException, InterruptedException
	{
		for(Future f : futures.keySet())
		{
			f.get();
			futures.remove(f);
		}
	}
	
	@Override
	public void shutdown()
	{
		pool.shutdown();
	}

	@Override
	public List<Runnable> shutdownNow()
	{
		return pool.shutdownNow();
	}

	@Override
	public boolean isShutdown()
	{
		return pool.isShutdown();
	}

	@Override
	public boolean isTerminated()
	{
		return pool.isTerminated();
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
	{
		return pool.awaitTermination(timeout, unit);
	}

	@Override
	public <T> Future<T> submit(Callable<T> task)
	{
		Future<T> f = pool.submit(task);
		futures.put(f, f);
		return f;
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result)
	{
		Future<T> f = pool.submit(task, result);
		futures.put(f, f);
		return f;
	}

	@Override
	public Future<?> submit(Runnable task)
	{
		Future<?> f = pool.submit(task);
		futures.put(f, f);
		return f;
	}

	@Deprecated
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
	{
		throw new RuntimeException("Not supported");
	}

	@Deprecated
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException
	{
		throw new RuntimeException("Not supported");
	}

	@Deprecated
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
	{
		throw new RuntimeException("Not supported");
	}

	@Deprecated
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
	{
		throw new RuntimeException("Not supported");
	}

	@Deprecated
	@Override
	public void execute(Runnable command)
	{
		throw new RuntimeException("Not supported");
	}
	
}
