package edu.coursera.parallel;

import java.util.concurrent.Phaser;

import static java.lang.Thread.currentThread;

/**
 * Wrapper class for implementing one-dimensional iterative averaging using
 * phasers.
 */
public final class OneDimAveragingPhaser {
    /**
     * Default constructor.
     */
    private OneDimAveragingPhaser() {
    }

    /**
     * Sequential implementation of one-dimensional iterative averaging.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     */
    public static void runSequential(final int iterations, final double[] myNew,
            final double[] myVal, final int n) {
        double[] next = myNew;
        double[] curr = myVal;

        for (int iter = 0; iter < iterations; iter++) {
            for (int j = 1; j <= n; j++) {
                next[j] = getReferenceElementByGroup(curr, j);
            }
            double[] tmp = curr;
            curr = next;
            next = tmp;
        }
    }

    /**
     * An example parallel implementation of one-dimensional iterative averaging
     * that uses phasers as a simple barrier (arriveAndAwaitAdvance).
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *        iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
        Phaser ph = new Phaser(0);
        ph.bulkRegister(tasks);

        Thread[] threads = new Thread[tasks];

        for (int ii = 0; ii < tasks; ii++) {
            final int i = ii;

            threads[ii] = new Thread(() -> {
                double[] threadPrivateMyVal = myVal;
                double[] threadPrivateMyNew = myNew;

                final int chunkSize = (n + tasks - 1) / tasks;
                final int left = (i * chunkSize) + 1;
                int right = (left + chunkSize) - 1;
                if (right > n) right = n;

                for (int iter = 0; iter < iterations; iter++) {
                    for (int j = left; j <= right; j++) {
                        threadPrivateMyNew[j] = getReferenceElementByGroup(threadPrivateMyVal, j);
                    }
                    ph.arriveAndAwaitAdvance();

                    double[] temp = threadPrivateMyNew;
                    threadPrivateMyNew = threadPrivateMyVal;
                    threadPrivateMyVal = temp;
                }
            });
            threads[ii].start();
        }

        for (int ii = 0; ii < tasks; ii++) {
            try {
                threads[ii].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A parallel implementation of one-dimensional iterative averaging that
     * uses the Phaser.arrive and Phaser.awaitAdvance APIs to overlap
     * computation with barrier completion.
     *
     * @param iterations The number of iterations to run
     * @param myNew A double array that starts as the output array
     * @param myVal A double array that contains the initial input to the
     *              iterative averaging problem
     * @param n The size of this problem
     * @param tasks The number of threads/tasks to use to compute the solution
     */
    public static void runParallelFuzzyBarrier(final int iterations,
            final double[] myNew, final double[] myVal, final int n,
            final int tasks) {
        Phaser ph = new Phaser(0);
        ph.bulkRegister(tasks);

        Thread[] threads = new Thread[tasks];

        for(int ii=0; ii<tasks; ii++){
            threads[ii] = getThreadByCurrentTask(iterations, myNew, myVal, n, tasks, ph, ii);
            threads[ii].start();
        }

        evaluateThreadsJoin(tasks, threads);
    }

    private static Thread getThreadByCurrentTask(int iterations, double[] myNew, double[] myVal, int n, int tasks, Phaser ph, int i) {
        return new Thread(() -> {
            double[] currentVal = myVal;
            double[] newVal = myNew;

            for(int iter=0; iter < iterations; iter++){
                //Compute leftmost boundary element for group
                int leftIter = i * (n/tasks) +1;
                newVal[leftIter] = getReferenceElementByGroup(currentVal, leftIter);

                //Compute rightmost boundary element for group
                int rightIter = (i+1) * (n/tasks);
                newVal[rightIter] = getReferenceElementByGroup(currentVal, rightIter);

                //Signal arrival on phaser ph
                int currentPhase = ph.arrive();

                //Compute phase elements
                for(int j=leftIter+1; j <= rightIter-1; j++)
                    newVal[j] = getReferenceElementByGroup(currentVal, j);

                //Wait for previous phase to complete before advancing
                ph.awaitAdvance(currentPhase);

                //Swap
                double[] temp = newVal;
                newVal = currentVal;
                currentVal = temp;
            }

        });
    }

    private static double getReferenceElementByGroup(double[] currentVal, int reference) {
        return (currentVal[reference - 1] + currentVal[reference + 1]) / 2.0;
    }

    private static void evaluateThreadsJoin(int tasks, Thread[] threads) {
        for (int ii=0; ii<tasks; ii++){
            try {
                threads[ii].join();
            } catch (InterruptedException exception){
                exception.printStackTrace();
                currentThread().interrupt();
            }
        }
    }
}
