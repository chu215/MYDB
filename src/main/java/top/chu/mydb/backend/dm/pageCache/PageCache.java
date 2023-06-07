package top.chu.mydb.backend.dm.pageCache;

import top.chu.mydb.backend.dm.page.Page;
import top.chu.mydb.backend.utils.Panic;
import top.chu.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;
    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);
    void truncateByPgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if (!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int) (memory / PAGE_SIZE));
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if (!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new PageCacheImpl(raf, fc, (int) (memory / PAGE_SIZE));
    }

}
