package dev.digitaldragon;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Sorts.descending;

public class ExternalCrawlyProcessor {
    private static final int NUM_THREADS = 125;
    private static final int MAX_QUEUE_SIZE = 300;
    public static void main(String[] args) {
        MongoManager.initializeDb();
        MongoCollection<Document> processingCollection = MongoManager.getProcessingCollection();

        while (true) {
            List<Future<?>> futures = new ArrayList<>();
            ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
            String lastUrl = "";
            try (MongoCursor<Document> cursor = processingCollection.find().sort(descending("url")).iterator()) {
                while (cursor.hasNext()) {
                    futures.removeIf(Future::isDone);
                    while (futures.size() < MAX_QUEUE_SIZE) {
                        Document next = cursor.next();
                        String url = next.getString("url");
                        processingCollection.findOneAndDelete(next);
                        if (url.equals(lastUrl))
                            continue;
                        lastUrl = url;
                        futures.add(executorService.submit(new ProcessTask(next)));
                    }
                }
            }
        }
    }

    final static class ProcessTask implements Callable<Void> {
        private final Document document;
        MongoCollection<Document> rejectsCollection = MongoManager.getRejectsCollection();
        MongoCollection<Document> bigQueueCollection = MongoManager.getBigqueueCollection();
        MongoCollection<Document> queueCollection = MongoManager.getQueueCollection();
        MongoCollection<Document> doneCollection = MongoManager.getDoneCollection();
        MongoCollection<Document> outCollection = MongoManager.getOutCollection();

        private ProcessTask(Document document) {
            this.document = document;
        }

        @Override
        public Void call() {
            try {
                if (document == null || document.getString("url") == null)
                    return null;

                String url = document.getString("url");

                if (isDuplicate(url, bigQueueCollection) || isDuplicate(url, queueCollection) || isDuplicate(url, doneCollection) || isDuplicate(url, outCollection)) {
                    document.append("reject_reason", "DUPLICATE");
                    System.out.println("DUP: " + url);
                }
                else if (!isValidURL(url)) {
                    document.append("reject_reason", "INVALID_URL");
                    System.out.println("INV: " + url);
                }
                else if (!hasValidDNS(url)) {
                    document.append("reject_reason", "INVALID_DNS");
                    System.out.println("DNS: " + url);
                }

                document.append("processed_at", Instant.now());
                document.remove("_id");

                if (document.get("reject_reason") != null) {
                    document.append("status", "REJECTED");
                    rejectsCollection.insertOne(document);
                } else {
                    document.append("status", "QUEUED");
                    if (!document.containsKey("domain"))
                        document.append("domain", getDomainFromUrl(url)); //todo we already extract the domain when checking dns, shouldn't duplicate effort when we need to
                    bigQueueCollection.insertOne(document);
                    System.out.println("ok " + url);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }
    }


    private static boolean isDuplicate(String url, MongoCollection<Document> collection) {
        Bson filter = Filters.eq("url", url);
        return collection.find(filter).cursor().hasNext();
    }

    private static boolean isValidURL(String url) {
        try {
            URL processed = new URL(url);
            return Objects.equals(processed.getProtocol(), "http") || Objects.equals(processed.getProtocol(), "https");
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean hasValidDNS(String url) throws ExecutionException, InterruptedException { //todo move into DNSCache class and possibly rename to DNSChecker
        String hostname = null;
        try {
            hostname = new URL(url).getHost();
        } catch (MalformedURLException e) {
            return false;
        }

        return DNSCache.isOkay(hostname).get();
    }

    public static String getDomainFromUrl(String url) {
        try {
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host != null) {
                String[] parts = host.split("\\.");
                if (parts.length > 2 && parts[parts.length - 2].equals("co")) {
                    return parts[parts.length - 3] + "." + parts[parts.length - 2] + "." + parts[parts.length - 1];
                } else if (parts.length < 2) {
                    return url;
                } else {
                    return parts[parts.length - 2] + "." + parts[parts.length - 1];
                }
            }
        } catch (URISyntaxException | NullPointerException e) {
            //do nothing lol
        }
        return null;
    }
}