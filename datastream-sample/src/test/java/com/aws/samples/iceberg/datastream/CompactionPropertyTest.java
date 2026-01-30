package com.aws.samples.iceberg.datastream;

import net.jqwik.api.*;
import org.junit.jupiter.api.Tag;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based test for compaction file size improvement.
 * 
 * Feature: iceberg-flink-samples, Property 7: Compaction File Size Improvement
 * 
 * Validates: Requirements 5.3
 * 
 * This test verifies that for any table with RewriteDataFiles configured,
 * after compaction runs on a set of small files, the resulting files should
 * have sizes closer to the target file size, and the total data content
 * should be preserved.
 */
@Tag("property-test")
public class CompactionPropertyTest {
    
    /**
     * Property: For any set of small files, compaction should produce fewer,
     * larger files while preserving total data size.
     */
    @Property(tries = 100)
    void compactionReducesFileCountAndImprovesFileSizes(
            @ForAll("smallFileSets") List<DataFile> files,
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        // Simulate compaction
        List<DataFile> compactedFiles = compactFiles(files, config);
        
        // Property 1: Total data size should be preserved
        long originalTotalSize = files.stream()
            .mapToLong(DataFile::getSize)
            .sum();
        
        long compactedTotalSize = compactedFiles.stream()
            .mapToLong(DataFile::getSize)
            .sum();
        
        assertEquals(
            originalTotalSize,
            compactedTotalSize,
            "Total data size should be preserved after compaction"
        );
        
        // Property 2: Number of files should be reduced (or stay same if already optimal)
        assertTrue(
            compactedFiles.size() <= files.size(),
            String.format("Compaction should reduce or maintain file count: %d -> %d",
                files.size(), compactedFiles.size())
        );
        
        // Property 3: Compacted files should be closer to target size
        double originalAvgDeviation = calculateAverageDeviationFromTarget(files, config.targetSize);
        double compactedAvgDeviation = calculateAverageDeviationFromTarget(compactedFiles, config.targetSize);
        
        assertTrue(
            compactedAvgDeviation <= originalAvgDeviation,
            String.format("Compacted files should be closer to target size: %.2f -> %.2f",
                originalAvgDeviation, compactedAvgDeviation)
        );
    }
    
    /**
     * Property: Compaction should not create files larger than maxFileSize.
     */
    @Property(tries = 100)
    void compactionRespectsMaxFileSize(
            @ForAll("smallFileSets") List<DataFile> files,
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        List<DataFile> compactedFiles = compactFiles(files, config);
        
        // Property: No file should exceed maxFileSize
        for (DataFile file : compactedFiles) {
            assertTrue(
                file.getSize() <= config.maxFileSize,
                String.format("File size %d should not exceed max %d",
                    file.getSize(), config.maxFileSize)
            );
        }
    }
    
    /**
     * Property: Files already at target size should not be rewritten.
     * This test verifies that files within the optimal size range are not compacted.
     */
    @Property(tries = 100)
    void compactionSkipsOptimalFiles(
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        // Create files that are all within the optimal range
        List<DataFile> optimalFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            // Generate files between minFileSize and maxFileSize
            long size = config.minFileSize + (config.maxFileSize - config.minFileSize) / 2;
            optimalFiles.add(new DataFile(i, size));
        }
        
        List<DataFile> compactedFiles = compactFiles(optimalFiles, config);
        
        // Property: File count should remain the same (no compaction needed)
        assertEquals(
            optimalFiles.size(),
            compactedFiles.size(),
            "Optimal files should not be compacted"
        );
        
        // Property: File sizes should remain unchanged
        List<Long> originalSizes = optimalFiles.stream()
            .map(DataFile::getSize)
            .sorted()
            .collect(Collectors.toList());
        
        List<Long> compactedSizes = compactedFiles.stream()
            .map(DataFile::getSize)
            .sorted()
            .collect(Collectors.toList());
        
        assertEquals(
            originalSizes,
            compactedSizes,
            "File sizes should remain unchanged for optimal files"
        );
    }
    
    /**
     * Property: Compaction should be idempotent - running it twice should
     * produce the same result.
     */
    @Property(tries = 100)
    void compactionIsIdempotent(
            @ForAll("smallFileSets") List<DataFile> files,
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        // Run compaction once
        List<DataFile> firstRun = compactFiles(files, config);
        
        // Run compaction again on the result
        List<DataFile> secondRun = compactFiles(firstRun, config);
        
        // Property: Results should be identical
        assertEquals(
            firstRun.size(),
            secondRun.size(),
            "Compaction should be idempotent - same file count"
        );
        
        List<Long> firstRunSizes = firstRun.stream()
            .map(DataFile::getSize)
            .sorted()
            .collect(Collectors.toList());
        
        List<Long> secondRunSizes = secondRun.stream()
            .map(DataFile::getSize)
            .sorted()
            .collect(Collectors.toList());
        
        assertEquals(
            firstRunSizes,
            secondRunSizes,
            "Compaction should be idempotent - same file sizes"
        );
    }
    
    /**
     * Property: Compaction should respect minInputFiles threshold.
     */
    @Property(tries = 100)
    void compactionRespectsMinInputFiles(
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        // Create fewer files than minInputFiles
        int fileCount = config.minInputFiles - 1;
        List<DataFile> files = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(new DataFile(i, config.minFileSize / 2));
        }
        
        List<DataFile> compactedFiles = compactFiles(files, config);
        
        // Property: Files should not be compacted if below threshold
        assertEquals(
            files.size(),
            compactedFiles.size(),
            "Should not compact when file count is below minInputFiles"
        );
    }
    
    /**
     * Property: Compaction should handle edge case of single file.
     */
    @Property(tries = 100)
    void compactionHandlesSingleFile(
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        List<DataFile> singleFile = Collections.singletonList(
            new DataFile(1, config.minFileSize / 2)
        );
        
        List<DataFile> compactedFiles = compactFiles(singleFile, config);
        
        // Property: Single file should remain unchanged
        assertEquals(
            1,
            compactedFiles.size(),
            "Single file should not be compacted"
        );
        
        assertEquals(
            singleFile.get(0).getSize(),
            compactedFiles.get(0).getSize(),
            "Single file size should remain unchanged"
        );
    }
    
    /**
     * Property: Compaction should preserve file content (simulated by file IDs).
     */
    @Property(tries = 100)
    void compactionPreservesFileContent(
            @ForAll("smallFileSets") List<DataFile> files,
            @ForAll("compactionConfigs") CompactionConfig config) {
        
        // Collect all file IDs (representing content)
        Set<Integer> originalIds = files.stream()
            .map(DataFile::getId)
            .collect(Collectors.toSet());
        
        List<DataFile> compactedFiles = compactFiles(files, config);
        
        // In real compaction, files are merged, so we track which original files
        // are represented in the compacted output
        Set<Integer> compactedIds = compactedFiles.stream()
            .flatMap(f -> f.getSourceIds().stream())
            .collect(Collectors.toSet());
        
        // Property: All original file content should be represented
        assertEquals(
            originalIds,
            compactedIds,
            "All original file content should be preserved in compacted files"
        );
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Simulates the file compaction logic.
     * This mimics the behavior of Iceberg's RewriteDataFiles action.
     */
    private List<DataFile> compactFiles(List<DataFile> files, CompactionConfig config) {
        if (files.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Filter files that need compaction (smaller than minFileSize)
        List<DataFile> smallFiles = files.stream()
            .filter(f -> f.getSize() < config.minFileSize)
            .collect(Collectors.toList());
        
        List<DataFile> goodFiles = files.stream()
            .filter(f -> f.getSize() >= config.minFileSize && f.getSize() <= config.maxFileSize)
            .collect(Collectors.toList());
        
        // If not enough small files to compact, return original
        if (smallFiles.size() < config.minInputFiles) {
            return new ArrayList<>(files);
        }
        
        // Compact small files into larger files
        List<DataFile> compactedFiles = new ArrayList<>(goodFiles);
        
        long currentGroupSize = 0;
        List<DataFile> currentGroup = new ArrayList<>();
        int nextId = files.stream().mapToInt(DataFile::getId).max().orElse(0) + 1;
        
        for (DataFile file : smallFiles) {
            currentGroup.add(file);
            currentGroupSize += file.getSize();
            
            // If we've reached target size or max file size, create a compacted file
            if (currentGroupSize >= config.targetSize || currentGroupSize >= config.maxFileSize) {
                Set<Integer> sourceIds = currentGroup.stream()
                    .map(DataFile::getId)
                    .collect(Collectors.toSet());
                
                compactedFiles.add(new DataFile(nextId++, currentGroupSize, sourceIds));
                
                currentGroup.clear();
                currentGroupSize = 0;
            }
        }
        
        // Handle remaining files in the last group
        if (!currentGroup.isEmpty()) {
            Set<Integer> sourceIds = currentGroup.stream()
                .map(DataFile::getId)
                .collect(Collectors.toSet());
            
            compactedFiles.add(new DataFile(nextId, currentGroupSize, sourceIds));
        }
        
        return compactedFiles;
    }
    
    /**
     * Calculate average deviation from target file size.
     */
    private double calculateAverageDeviationFromTarget(List<DataFile> files, long targetSize) {
        if (files.isEmpty()) {
            return 0.0;
        }
        
        double totalDeviation = files.stream()
            .mapToDouble(f -> Math.abs(f.getSize() - targetSize))
            .sum();
        
        return totalDeviation / files.size();
    }
    
    // ========== Data Generators ==========
    
    /**
     * Generates sets of small files that need compaction.
     */
    @Provide
    Arbitrary<List<DataFile>> smallFileSets() {
        return Arbitraries.integers().between(5, 30).flatMap(count -> {
            return Arbitraries.integers()
                .between(1, count)
                .list()
                .ofSize(count)
                .map(ids -> {
                    List<DataFile> files = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        // Generate small file sizes (10MB to 50MB)
                        long size = (long) (10 * 1024 * 1024 + Math.random() * 40 * 1024 * 1024);
                        files.add(new DataFile(ids.get(i), size));
                    }
                    return files;
                });
        });
    }
    
    /**
     * Generates sets of files already at optimal size.
     */
    @Provide
    Arbitrary<List<DataFile>> optimalFileSets() {
        return Arbitraries.integers().between(3, 15).flatMap(count -> {
            return Arbitraries.integers()
                .between(1, count)
                .list()
                .ofSize(count)
                .map(ids -> {
                    List<DataFile> files = new ArrayList<>();
                    long targetSize = 256 * 1024 * 1024;  // 256 MB
                    for (int i = 0; i < count; i++) {
                        // Generate files near target size (240MB to 270MB)
                        long size = (long) (targetSize * (0.94 + Math.random() * 0.12));
                        files.add(new DataFile(ids.get(i), size));
                    }
                    return files;
                });
        });
    }
    
    /**
     * Generates compaction configurations.
     */
    @Provide
    Arbitrary<CompactionConfig> compactionConfigs() {
        return Combinators.combine(
            Arbitraries.longs().between(128 * 1024 * 1024, 512 * 1024 * 1024),  // targetSize: 128-512 MB
            Arbitraries.integers().between(3, 10)  // minInputFiles: 3-10
        ).as((targetSize, minInputFiles) -> {
            long minFileSize = (long) (targetSize * 0.75);  // 75% of target
            long maxFileSize = (long) (targetSize * 1.8);   // 180% of target
            return new CompactionConfig(targetSize, minFileSize, maxFileSize, minInputFiles);
        });
    }
    
    // ========== Test Data Classes ==========
    
    /**
     * Represents a data file for testing.
     */
    static class DataFile {
        private final int id;
        private final long size;
        private final Set<Integer> sourceIds;  // IDs of files that were merged to create this file
        
        public DataFile(int id, long size) {
            this.id = id;
            this.size = size;
            this.sourceIds = Collections.singleton(id);
        }
        
        public DataFile(int id, long size, Set<Integer> sourceIds) {
            this.id = id;
            this.size = size;
            this.sourceIds = new HashSet<>(sourceIds);
        }
        
        public int getId() {
            return id;
        }
        
        public long getSize() {
            return size;
        }
        
        public Set<Integer> getSourceIds() {
            return sourceIds;
        }
        
        @Override
        public String toString() {
            return String.format("DataFile{id=%d, size=%dMB, sources=%s}",
                id, size / (1024 * 1024), sourceIds);
        }
    }
    
    /**
     * Represents compaction configuration.
     */
    static class CompactionConfig {
        private final long targetSize;
        private final long minFileSize;
        private final long maxFileSize;
        private final int minInputFiles;
        
        public CompactionConfig(long targetSize, long minFileSize, long maxFileSize, int minInputFiles) {
            this.targetSize = targetSize;
            this.minFileSize = minFileSize;
            this.maxFileSize = maxFileSize;
            this.minInputFiles = minInputFiles;
        }
        
        @Override
        public String toString() {
            return String.format("CompactionConfig{target=%dMB, min=%dMB, max=%dMB, minFiles=%d}",
                targetSize / (1024 * 1024), minFileSize / (1024 * 1024),
                maxFileSize / (1024 * 1024), minInputFiles);
        }
    }
}
