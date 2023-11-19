package io.agroal.test.basic;

public abstract class Utils {
    /**
     * This method will start a daemon thread that will sleep indefinitely in order to be able to park with a higher
     * resolution for windows. Without this hack, LockSupport.parkNanos() will not be able to park for less than ~16ms
     * on windows.
     *
     * @see <a href="https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking-part-ii-windows/">blog</a>
     * @see <a href="https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6435126">jdk bug</a>
     */
    public static void windowsTimerHack() {
        Thread t = new Thread(() -> {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) { // a delicious interrupt, omm, omm
            }
        });
        t.setDaemon(true);
        t.start();
    }
}
