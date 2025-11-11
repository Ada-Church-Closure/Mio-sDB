package top.guoziyang.mydb.backend.dm.pageIndex;

// PageInfo表示页面的信息,包括页面号和空闲空间大小
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
