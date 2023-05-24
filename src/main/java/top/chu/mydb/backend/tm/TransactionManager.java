package top.chu.mydb.backend.tm;

public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort()
}
