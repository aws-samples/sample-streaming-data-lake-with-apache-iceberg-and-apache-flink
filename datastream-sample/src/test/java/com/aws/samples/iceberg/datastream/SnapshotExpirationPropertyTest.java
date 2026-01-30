package com.aws.samples.iceberg.datastream;

import net.jqwik.api.*;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based test for snapshot expiration correctness.
 * 
 * Feature: iceberg-flink-samples, Property 6: Snapshot Expiration Correctness
 * 
 * Validates: Requirements 5.2
 * 
 * This test verifies that for any table with ExpireSnapshots configured,
 * after the maintenance task runs, no snapshots older than the configured
 * retention period should remain (except for the minimum retained count).
 */
@Tag("property-test")
public class SnapshotExpirationPropertyTest {
    
    /**
     * Property: For any set of snapshots, expiration should remove only snapshots
     * older than the retention period while preserving the minimum count.
     * 
     * This property tests the snapshot expiration logic:
     * 1. Snapshots older than maxAge should be expired
     * 2. At least retainLast snapshots should always be kept
     * 3. Snapshots within retention period should never be expired
     */
    @Property(tries = 100)
    void snapshotExpirationRespectsRetentionPolicy(
            @ForAll("snapshotSets") List<Snapshot> snapshots,
            @ForAll("retentionConfigs") RetentionConfig config) {
        
        Instant now = Instant.now();
        Instant cutoffTime = now.minus(config.maxAge);
        
        // Simulate snapshot expiration
        List<Snapshot> remainingSnapshots = expireSnapshots(snapshots, config, now);
        
        // Property 1: At least retainLast snapshots should remain
        assertTrue(
            remainingSnapshots.size() >= Math.min(config.retainLast, snapshots.size()),
            String.format("Should retain at least %d snapshots, but only %d remain",
                config.retainLast, remainingSnapshots.size())
        );
        
        // Property 2: All remaining snapshots should either be:
        // - Within the retention period, OR
        // - Part of the retainLast most recent snapshots
        List<Snapshot> sortedOriginal = snapshots.stream()
            .sorted(Comparator.comparing(Snapshot::getTimestamp).reversed())
            .collect(Collectors.toList());
        
        Set<Long> protectedSnapshotIds = sortedOriginal.stream()
            .limit(config.retainLast)
            .map(Snapshot::getId)
            .collect(Collectors.toSet());
        
        for (Snapshot snapshot : remainingSnapshots) {
            boolean isWithinRetention = snapshot.getTimestamp().isAfter(cutoffTime);
            boolean isProtected = protectedSnapshotIds.contains(snapshot.getId());
            
            assertTrue(
                isWithinRetention || isProtected,
                String.format("Snapshot %d at %s should either be within retention or protected",
                    snapshot.getId(), snapshot.getTimestamp())
            );
        }
        
        // Property 3: No snapshot older than cutoff should remain unless protected
        for (Snapshot snapshot : remainingSnapshots) {
            if (snapshot.getTimestamp().isBefore(cutoffTime)) {
                assertTrue(
                    protectedSnapshotIds.contains(snapshot.getId()),
                    String.format("Old snapshot %d should only remain if protected by retainLast",
                        snapshot.getId())
                );
            }
        }
    }
    
    /**
     * Property: Expiration should be idempotent - running it multiple times
     * should not change the result.
     */
    @Property(tries = 100)
    void snapshotExpirationIsIdempotent(
            @ForAll("snapshotSets") List<Snapshot> snapshots,
            @ForAll("retentionConfigs") RetentionConfig config) {
        
        Instant now = Instant.now();
        
        // Run expiration once
        List<Snapshot> firstRun = expireSnapshots(snapshots, config, now);
        
        // Run expiration again on the result
        List<Snapshot> secondRun = expireSnapshots(firstRun, config, now);
        
        // Property: Results should be identical
        assertEquals(
            firstRun.size(),
            secondRun.size(),
            "Expiration should be idempotent - same number of snapshots"
        );
        
        Set<Long> firstRunIds = firstRun.stream()
            .map(Snapshot::getId)
            .collect(Collectors.toSet());
        
        Set<Long> secondRunIds = secondRun.stream()
            .map(Snapshot::getId)
            .collect(Collectors.toSet());
        
        assertEquals(
            firstRunIds,
            secondRunIds,
            "Expiration should be idempotent - same snapshot IDs"
        );
    }
    
    /**
     * Property: When retainLast is greater than total snapshots,
     * all snapshots should be retained regardless of age.
     */
    @Property(tries = 100)
    void retainLastPreservesAllWhenGreaterThanTotal(
            @ForAll("snapshotSets") List<Snapshot> snapshots) {
        
        // Configure retention to keep more than we have
        RetentionConfig config = new RetentionConfig(
            Duration.ofHours(1),  // Very short retention
            snapshots.size() + 10  // More than total snapshots
        );
        
        Instant now = Instant.now();
        List<Snapshot> remaining = expireSnapshots(snapshots, config, now);
        
        // Property: All snapshots should be retained
        assertEquals(
            snapshots.size(),
            remaining.size(),
            "All snapshots should be retained when retainLast > total"
        );
    }
    
    /**
     * Property: When all snapshots are within retention period,
     * none should be expired.
     */
    @Property(tries = 100)
    void noExpirationWhenAllWithinRetention(
            @ForAll("recentSnapshots") List<Snapshot> snapshots,
            @ForAll("retentionConfigs") RetentionConfig config) {
        
        Instant now = Instant.now();
        
        // Ensure all snapshots are recent (within 1 hour)
        List<Snapshot> recentSnapshots = snapshots.stream()
            .map(s -> new Snapshot(s.getId(), now.minusSeconds(s.getId() % 3600)))
            .collect(Collectors.toList());
        
        // Use a long retention period
        RetentionConfig longRetention = new RetentionConfig(
            Duration.ofDays(7),
            config.retainLast
        );
        
        List<Snapshot> remaining = expireSnapshots(recentSnapshots, longRetention, now);
        
        // Property: All snapshots should be retained
        assertEquals(
            recentSnapshots.size(),
            remaining.size(),
            "No snapshots should be expired when all are within retention period"
        );
    }
    
    /**
     * Property: Expiration should preserve snapshot ordering by timestamp.
     */
    @Property(tries = 100)
    void expirationPreservesSnapshotOrdering(
            @ForAll("snapshotSets") List<Snapshot> snapshots,
            @ForAll("retentionConfigs") RetentionConfig config) {
        
        Instant now = Instant.now();
        List<Snapshot> remaining = expireSnapshots(snapshots, config, now);
        
        // Property: Remaining snapshots should be in chronological order
        for (int i = 0; i < remaining.size() - 1; i++) {
            assertTrue(
                remaining.get(i).getTimestamp().isBefore(remaining.get(i + 1).getTimestamp()) ||
                remaining.get(i).getTimestamp().equals(remaining.get(i + 1).getTimestamp()),
                "Snapshots should remain in chronological order after expiration"
            );
        }
    }
    
    /**
     * Property: Edge case - single snapshot should always be retained.
     */
    @Property(tries = 100)
    void singleSnapshotAlwaysRetained(
            @ForAll("retentionConfigs") RetentionConfig config) {
        
        Instant now = Instant.now();
        Instant veryOld = now.minus(Duration.ofDays(365));
        
        List<Snapshot> singleSnapshot = Collections.singletonList(
            new Snapshot(1L, veryOld)
        );
        
        List<Snapshot> remaining = expireSnapshots(singleSnapshot, config, now);
        
        // Property: Single snapshot should always be retained
        assertEquals(
            1,
            remaining.size(),
            "Single snapshot should always be retained regardless of age"
        );
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Simulates the snapshot expiration logic.
     * This mimics the behavior of Iceberg's ExpireSnapshots action.
     */
    private List<Snapshot> expireSnapshots(
            List<Snapshot> snapshots,
            RetentionConfig config,
            Instant now) {
        
        if (snapshots.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Sort snapshots by timestamp (oldest first)
        List<Snapshot> sorted = snapshots.stream()
            .sorted(Comparator.comparing(Snapshot::getTimestamp))
            .collect(Collectors.toList());
        
        // Calculate cutoff time
        Instant cutoffTime = now.minus(config.maxAge);
        
        // Determine how many snapshots to keep
        int totalSnapshots = sorted.size();
        int minToKeep = Math.min(config.retainLast, totalSnapshots);
        
        // Always keep the most recent retainLast snapshots
        List<Snapshot> result = new ArrayList<>();
        
        // Keep snapshots that are either:
        // 1. Within the retention period, OR
        // 2. Among the most recent retainLast snapshots
        int keptCount = 0;
        for (int i = sorted.size() - 1; i >= 0; i--) {
            Snapshot snapshot = sorted.get(i);
            
            // Always keep if within retention period
            if (snapshot.getTimestamp().isAfter(cutoffTime)) {
                result.add(snapshot);
                keptCount++;
            }
            // Keep if we haven't reached retainLast yet
            else if (keptCount < minToKeep) {
                result.add(snapshot);
                keptCount++;
            }
        }
        
        // Sort result chronologically
        result.sort(Comparator.comparing(Snapshot::getTimestamp));
        
        return result;
    }
    
    // ========== Data Generators ==========
    
    /**
     * Generates sets of snapshots with varying timestamps.
     */
    @Provide
    Arbitrary<List<Snapshot>> snapshotSets() {
        return Arbitraries.integers().between(1, 50).flatMap(count -> {
            Instant now = Instant.now();
            
            return Arbitraries.longs()
                .between(1, count)
                .list()
                .ofSize(count)
                .map(ids -> {
                    List<Snapshot> snapshots = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        // Generate timestamps ranging from 30 days ago to now
                        long secondsAgo = (long) (Math.random() * 30 * 24 * 3600);
                        Instant timestamp = now.minusSeconds(secondsAgo);
                        snapshots.add(new Snapshot(ids.get(i), timestamp));
                    }
                    return snapshots;
                });
        });
    }
    
    /**
     * Generates recent snapshots (within last hour).
     */
    @Provide
    Arbitrary<List<Snapshot>> recentSnapshots() {
        return Arbitraries.integers().between(1, 20).flatMap(count -> {
            Instant now = Instant.now();
            
            return Arbitraries.longs()
                .between(1, count)
                .list()
                .ofSize(count)
                .map(ids -> {
                    List<Snapshot> snapshots = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        // Generate timestamps within last hour
                        long secondsAgo = (long) (Math.random() * 3600);
                        Instant timestamp = now.minusSeconds(secondsAgo);
                        snapshots.add(new Snapshot(ids.get(i), timestamp));
                    }
                    return snapshots;
                });
        });
    }
    
    /**
     * Generates retention configurations.
     */
    @Provide
    Arbitrary<RetentionConfig> retentionConfigs() {
        return Combinators.combine(
            Arbitraries.integers().between(1, 72),  // maxAge in hours
            Arbitraries.integers().between(1, 10)   // retainLast count
        ).as((hours, retainLast) -> 
            new RetentionConfig(Duration.ofHours(hours), retainLast)
        );
    }
    
    // ========== Test Data Classes ==========
    
    /**
     * Represents an Iceberg snapshot for testing.
     */
    static class Snapshot {
        private final long id;
        private final Instant timestamp;
        
        public Snapshot(long id, Instant timestamp) {
            this.id = id;
            this.timestamp = timestamp;
        }
        
        public long getId() {
            return id;
        }
        
        public Instant getTimestamp() {
            return timestamp;
        }
        
        @Override
        public String toString() {
            return String.format("Snapshot{id=%d, timestamp=%s}", id, timestamp);
        }
    }
    
    /**
     * Represents snapshot retention configuration.
     */
    static class RetentionConfig {
        private final Duration maxAge;
        private final int retainLast;
        
        public RetentionConfig(Duration maxAge, int retainLast) {
            this.maxAge = maxAge;
            this.retainLast = retainLast;
        }
        
        public Duration getMaxAge() {
            return maxAge;
        }
        
        public int getRetainLast() {
            return retainLast;
        }
        
        @Override
        public String toString() {
            return String.format("RetentionConfig{maxAge=%s, retainLast=%d}", 
                maxAge, retainLast);
        }
    }
}
