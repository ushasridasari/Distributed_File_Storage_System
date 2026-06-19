package com.gfs;

import com.gfs.common.FileMetadata;
import com.gfs.master.Namespace;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class NamespaceTest {
    private Namespace ns;

    @Before
    public void setUp() {
        ns = new Namespace();
    }

    @Test
    public void testCreateAndGet() {
        ns.create("/foo/bar.txt");
        assertTrue(ns.exists("/foo/bar.txt"));
        FileMetadata meta = ns.get("/foo/bar.txt");
        assertNotNull(meta);
        assertEquals("/foo/bar.txt", meta.getPath());
    }

    @Test(expected = IllegalStateException.class)
    public void testDuplicateCreateThrows() {
        ns.create("/dup.txt");
        ns.create("/dup.txt");
    }

    @Test
    public void testDelete() {
        ns.create("/to-delete.txt");
        FileMetadata removed = ns.delete("/to-delete.txt");
        assertNotNull(removed);
        assertFalse(ns.exists("/to-delete.txt"));
    }

    @Test
    public void testList() {
        ns.create("/data/a.txt");
        ns.create("/data/b.txt");
        ns.create("/other/c.txt");
        List<String> results = ns.list("/data/");
        assertEquals(2, results.size());
        assertTrue(results.contains("/data/a.txt"));
        assertTrue(results.contains("/data/b.txt"));
    }

    @Test
    public void testAddChunk() {
        FileMetadata meta = ns.create("/chunked.txt");
        meta.addChunk("chunk-001");
        meta.addChunk("chunk-002");
        assertEquals(2, meta.getChunkIds().size());
    }
}
