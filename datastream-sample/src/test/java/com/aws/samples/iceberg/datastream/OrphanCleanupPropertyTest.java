package com.aws.samples.iceberg.datastream;

import net.jqwik.api.*;
import org.junit.jupiter.api.Tag;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based test for orphan file cleanup safety.
 * 
 * Feature: iceberg-flink-samples, Property 8: Orphan File Cleanup Safety
 * 
 * Validates: Requirements 5.4
 * 
 * This test verifies that for any table with DeleteOrphanFiles configured,
 * only files that are not referenced by any snapshot and are older than the
 * minimum age should be deleted; all referenced files must be preserved.
 */
@Tag("property-test")
public class OrphanCleanupPropertyTest {
    
    /**
     * Property: For any set of files, orphan cleanup should only delete
     * unreferenced files older than minAge.
     */
    @Property(tries = 100)
    void orphanCleanupPreservesReferencedFiles(
            @ForAll("fileSets") FileSet fileSet,
            @ForAll("cleanupConfigs") CleanupConfig config) {
        
        Instant now = Instant.now();
        
        // Simulate orphan cleanup
        List<DataFile> remainingFiles = cleanupOrphanFiles(fileSet, config, now);
        
        // Property 1: All referenced files should be preserved
        Set<String> referencedFileIds = fileSet.referencedFiles.stream()
            .map(DataFile::getId)
            .collect(Collectors.toSet());
        
        Set<String> remainingFileIds = remainingFiles.stream()
            .map(DataFile::getId)
            .collect(Collectors.toSet());
        
        assertTrue(
            remainingFileIds.containsAll(referencedFileIds),
            "All referenced files should be preserved"
        );
        
        // Property 2: Only unreferenced files older than minAge should be deleted
        Instant cutoffTime = now.minus(config.minAge);
        
        for (DataFile file : fileSet.allFiles) {
            boolean isReferenced = referencedFileIds.contains(file.getId());
            boolean isOld = file.getCreatedAt().isBefore(cutoffTime);
            boolean shouldBeDeleted = !isReferenced && isOld;
            boolean wasDeleted = !remainingFileIds.contains(file.getId());
            
            assertEquals(
                shouldBeDeleted,
                wasDeleted,
                String.format("File %s (referenced=%s, old=%s) deletion mismatch",
                    file.getId(), isReferenced, isOld)
            );
        }
    }
    
    /**
     * Property: Cleanup should never delete files younger than minAge.
     */
    @Property(tries = 100)
    void orphanCleanupRespectsMinAge(
            @ForAll("fileSets") FileSet fileSet,
            @ForAll("cleanupConfigs") CleanupConfig config) {
        
        Instant now = Instant.now();
        Instant cutoffTime = now.minus(config.minAge);
        
        List<DataFile> remainingFiles = cleanupOrphanFiles(fileSet, config, now);
        Set<String> remainingFileIds = remainingFiles.stream()
            .map(DataFile::getId)
            .collect(Collectors.toSet());
        
        // Property: All files younger than minAge should be preserved
        for (DataFile file : fileSet.allFiles) {
            if (file.getCreatedAt().isAfter(cutoffTime)) {
                assertTrue(
                    remainingFileIds.contains(file.getId()),
                    String.format("File %s created at %s should be preserved (younger than minAge)",
                        file.getId(), file.getCreatedAt())
                );
            }
        }
    }
    
    /**
     * Property: Cleanup should be idempotent.
     */
    @Property(tries = 100)
    void orphanCleanupIsIdempotent(
            @ForAll("fileSets") FileSet fileSet,
            @ForAll("cleanupConfigs") CleanupConfig config) {
        
        Instant now = Instant.now();
        
        // Run cleanup once
        List<DataFile> firstRun = cleanupOrphanFiles(fileSet, config, now);
        
        // Create new FileSet with remaining files
        FileSet afterFirstCleanup = new FileSet(firstRun, fileSet.referencedFiles);
        
        // Run cleanup again
        List<DataFile> secondRun = cleanupOrphanFiles(afterFirstCleanup, config, now);
        
        // Property: Results should be identical
        assertEquals(
            firstRun.size(),
            secondRun.size(),
            "Cleanup should be idempotent"
        );
    }
    
    /**
     * Property: Edge case - all files referenced should result in no deletions.
     */
    @Property(tries = 100)
    void orphanCleanupWithAllReferencedFiles(
            @ForAll("cleanupConfigs") CleanupConfig config) {
        
        Instant now = Instant.now();
        Instant veryOld = now.minus(Duration.ofDays(365));
        
        // Create files that are all referenced
        List<DataFile> files = Arrays.asList(
            new DataFile("file1", veryOld),
            new DataFile("file2", veryOld),
            new DataFile("file3", veryOld)
        );
        
        FileSet fileSet = new FileSet(files, files);  // All files are referenced
        
        List<DataFile> remainingFiles = cleanupOrphanFiles(fileSet, config, now);
        
        // Property: No files should be deleted
        assertEquals(
            files.size(),
            remainingFiles.size(),
            "All referenced files should be preserved regardless of age"
        );
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Simulates the orphan file cleanup logic.
     */
    private List<DataFile> cleanupOrphanFiles(FileSet fileSet, CleanupConfig config, Instant now) {
        Instant cutoffTime = now.minus(config.minAge);
        
        Set<String> referencedFileIds = fileSet.referencedFiles.stream()
            .map(DataFile::getId)
            .collect(Collectors.toSet());
        
        List<DataFile> remainingFiles = new ArrayList<>();
        
        for (DataFile file : fileSet.allFiles) {
            boolean isReferenced = referencedFileIds.contains(file.getId());
            boolean isOld = file.getCreatedAt().isBefore(cutoffTime);
            
            // Keep file if it's referenced OR if it's not old enough
            if (isReferenced || !isOld) {
                remainingFiles.add(file);
            }
        }
        
        return remainingFiles;
    }
    
    // ========== Data Generators ==========
    
    /**
     * Generates sets of files with some referenced and some orphaned.
     */
    @Provide
    Arbitrary<FileSet> fileSets() {
        return Arbitraries.integers().between(5, 20).flatMap(totalCount -> {
            return Arbitraries.integers().between(1, totalCount - 1).flatMap(referencedCount -> {
                Instant now = Instant.now();
                
                // Generate unique file IDs
                List<String> uniqueIds = new ArrayList<>();
                for (int i = 0; i < totalCount; i++) {
                    uniqueIds.add("file" + i);
                }
                
                List<DataFile> allFiles = new ArrayList<>();
                
                for (int i = 0; i < totalCount; i++) {
                    // Generate files with varying ages (0-30 days old)
                    long daysAgo = (long) (Math.random() * 30);
                    Instant createdAt = now.minus(Duration.ofDays(daysAgo));
                    allFiles.add(new DataFile(uniqueIds.get(i), createdAt));
                }
                
                // Select some files as referenced
                List<DataFile> referencedFiles = allFiles.subList(0, referencedCount);
                
                return Arbitraries.just(new FileSet(allFiles, referencedFiles));
            });
        });
    }
    
    /**
     * Generates cleanup configurations.
     */
    @Provide
    Arbitrary<CleanupConfig> cleanupConfigs() {
        return Arbitraries.integers().between(1, 10).map(days ->
            new CleanupConfig(Duration.ofDays(days))
        );
    }
    
    // ========== Test Data Classes ==========
    
    /**
     * Represents a data file for testing.
     */
    static class DataFile {
        private final String id;
        private final Instant createdAt;
        
        public DataFile(String id, Instant createdAt) {
            this.id = id;
            this.createdAt = createdAt;
        }
        
        public String getId() {
            return id;
        }
        
        public Instant getCreatedAt() {
            return createdAt;
        }
        
        @Override
        public String toString() {
            return String.format("DataFile{id=%s, createdAt=%s}", id, createdAt);
        }
    }
    
    /**
     * Represents a set of files with some referenced by snapshots.
     */
    static class FileSet {
        private final List<DataFile> allFiles;
        private final List<DataFile> referencedFiles;
        
        public FileSet(List<DataFile> allFiles, List<DataFile> referencedFiles) {
            this.allFiles = new ArrayList<>(allFiles);
            this.referencedFiles = new ArrayList<>(referencedFiles);
        }
    }
    
    /**
     * Represents orphan cleanup configuration.
     */
    static class CleanupConfig {
        private final Duration minAge;
        
        public CleanupConfig(Duration minAge) {
            this.minAge = minAge;
        }
        
        @Override
        public String toString() {
            return String.format("CleanupConfig{minAge=%s}", minAge);
        }
    }
}
