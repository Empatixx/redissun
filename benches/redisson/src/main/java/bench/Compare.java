package bench;

import org.redisson.Redisson;
import org.redisson.api.RBatch;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

/**
 * The same workload as benches/compare.rs, on Redisson. Redisson runs with its defaults, except
 * the codec, which is JSON like in the Rust runs. It has a blocking API, so the concurrent run
 * uses 64 threads.
 *
 * mvn -q compile exec:java -Dexec.args="redis://127.0.0.1:6379 ../results/redisson.json"
 */
public class Compare {
    static final int CONCURRENCY = 64;
    static final int BATCH = 100;
    static final String FIELD = "field";
    static final long WARMUP_NS = 5_000_000_000L;
    static final long MEASURE_NS = 3_000_000_000L;

    public static class User {
        public long id = 42;
        public String name = "Jirka Novak";
        public String email = "jirka@example.com";
        public boolean active = true;
    }

    static String key(String run, String kind, int slot) {
        return "bench:" + run + ":redisson:" + kind + ":" + slot;
    }

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "redis://127.0.0.1:6379";
        String out = args.length > 1 ? args[1] : "../results/redisson.json";
        String only = System.getenv("REDISSUN_BENCH_ONLY");
        String run = Long.toHexString(System.nanoTime());

        Config config = new Config();
        config.setCodec(new JsonJacksonCodec());
        config.useSingleServer().setAddress(url);
        RedissonClient client = Redisson.create(config);
        User user = new User();

        List<RBucket<User>> buckets = new ArrayList<>();
        List<RMap<String, User>> maps = new ArrayList<>();
        List<RLock> locks = new ArrayList<>();
        for (int slot = 0; slot < CONCURRENCY; slot++) {
            buckets.add(client.getBucket(key(run, "bucket", slot)));
            maps.add(client.getMap(key(run, "map", slot)));
            locks.add(client.getLock(key(run, "lock", slot)));
            buckets.get(slot).set(user);
            maps.get(slot).put(FIELD, user);
        }

        Map<String, IntConsumer> ops = new LinkedHashMap<>();
        ops.put("bucket_set", slot -> buckets.get(slot).set(user));
        ops.put("bucket_get", slot -> buckets.get(slot).get());
        ops.put("map_insert", slot -> maps.get(slot).put(FIELD, user));
        ops.put("map_get", slot -> maps.get(slot).get(FIELD));
        ops.put("lock_unlock", slot -> {
            RLock lock = locks.get(slot);
            lock.lock();
            lock.unlock();
        });
        ops.put("batch_100", slot -> {
            RBatch batch = client.createBatch();
            for (int i = 0; i < BATCH; i++) {
                batch.getBucket(key(run, "batch", i)).setAsync(user);
            }
            batch.execute();
        });

        List<Map<String, Object>> results = new ArrayList<>();
        for (Map.Entry<String, IntConsumer> entry : ops.entrySet()) {
            String name = entry.getKey();
            if (only != null && !name.contains(only)) continue;
            IntConsumer op = entry.getValue();

            for (long end = System.nanoTime() + WARMUP_NS; System.nanoTime() < end; ) op.accept(0);
            long[] samples = new long[2_000_000];
            int count = 0;
            for (long end = System.nanoTime() + MEASURE_NS; System.nanoTime() < end && count < samples.length; ) {
                long started = System.nanoTime();
                op.accept(0);
                samples[count++] = System.nanoTime() - started;
            }
            Arrays.sort(samples, 0, count);
            double p50 = samples[(int) ((count - 1) * 0.5)] / 1000.0;
            double p99 = samples[(int) ((count - 1) * 0.99)] / 1000.0;

            Double perSecond = null;
            if (!name.equals("batch_100")) {
                throughput(op, 3_000_000_000L);
                perSecond = throughput(op, MEASURE_NS);
            }
            System.err.printf("%-9s %-12s p50 %8.1f us  p99 %8.1f us  %s%n", "redisson", name, p50, p99,
                perSecond == null ? "" : String.format("%9.0f ops/s", perSecond));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("library", "redisson");
            row.put("operation", name);
            row.put("p50_us", p50);
            row.put("p99_us", p99);
            row.put("ops_per_second", perSecond);
            results.add(row);
        }

        StringBuilder json = new StringBuilder();
        json.append("{\n  \"workload\": {\"payload\": \"JSON object of about 80 bytes\", \"concurrency\": ")
            .append(CONCURRENCY).append(", \"batch_commands\": ").append(BATCH)
            .append(", \"measure_seconds\": ").append(MEASURE_NS / 1_000_000_000L).append("},\n  \"results\": [\n");
        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> row = results.get(i);
            json.append(String.format(java.util.Locale.ROOT,
                "    {\"library\": \"redisson\", \"operation\": \"%s\", \"p50_us\": %.3f, \"p99_us\": %.3f, \"ops_per_second\": %s}%s\n",
                row.get("operation"), row.get("p50_us"), row.get("p99_us"),
                row.get("ops_per_second") == null ? "null" : String.format(java.util.Locale.ROOT, "%.1f", row.get("ops_per_second")),
                i + 1 < results.size() ? "," : ""));
        }
        json.append("  ]\n}\n");
        File file = new File(out);
        file.getAbsoluteFile().getParentFile().mkdirs();
        java.nio.file.Files.writeString(file.toPath(), json.toString());
        System.err.println("wrote " + out);
        client.shutdown();
    }

    static double throughput(IntConsumer op, long durationNs) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        AtomicLong total = new AtomicLong();
        long started = System.nanoTime();
        long end = started + durationNs;
        List<Future<?>> tasks = new ArrayList<>();
        for (int slot = 0; slot < CONCURRENCY; slot++) {
            final int mine = slot;
            tasks.add(pool.submit(() -> {
                long done = 0;
                while (System.nanoTime() < end) {
                    op.accept(mine);
                    done++;
                }
                total.addAndGet(done);
            }));
        }
        for (Future<?> task : tasks) task.get();
        pool.shutdown();
        return total.get() / ((System.nanoTime() - started) / 1e9);
    }
}
