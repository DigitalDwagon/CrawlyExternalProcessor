package dev.digitaldragon;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class DNSCache {

    private static final ConcurrentHashMap<String, Future<Boolean>> okDomains = new ConcurrentHashMap<>();
    //private static final Object lock = new Object();

    public static Future<Boolean> isOkay(String domain) {
        try {
            // Attempt to return an existing Future if one exists
            Future<Boolean> existingFuture = okDomains.get(domain);
            if (existingFuture != null)
                return existingFuture;

            // Create new Callable and FutureTask for DNS lookup
            Callable<Boolean> callable = () -> {
                try {
                    // Perform DNS lookup using a blocking call
                    System.out.printf("Querying for domain %s%n", domain);
                    InetAddress address = InetAddress.getByName(domain);
                    if (address.isAnyLocalAddress()) {
                        throw new UnknownHostException();
                    }
                    return true;
                } catch (UnknownHostException ue) {
                    System.out.println("No valid DNS.");
                    return false;
                }
            };
            FutureTask<Boolean> futureTask = new FutureTask<>(callable);

            // Attempt to put the FutureTask in the map if no other thread has put a Future in the meantime
            existingFuture = okDomains.putIfAbsent(domain, futureTask);

            // If no other value was put by another thread, start the task
            if (existingFuture == null) {
                existingFuture = futureTask;
                new Thread(futureTask).start();
            }

            return existingFuture;
        } catch (Exception e) {
            throw new RuntimeException("Unknown error occurred while performing DNS lookup.", e);
        }
    }
}
