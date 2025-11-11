package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

//  这里的pageIndex,是一个散列桶,是一个在内存中工作的数据结构.
//  页面索引，缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，而无需从磁盘或者缓存中检查每一个页面的信息。
// 对于所有页面的索引,分类的根据就是当前页面持有的空闲块的大小.
// DB 用一个比较粗略的算法实现了页面索引，将一页的空间划分成了 40 个区间。在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。insert 在请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求。
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    // 一个区间的大小
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    // 保护页面索引的锁
    private Lock lock;
    // 每个区间对应的页面列表
    // 其实就是一个长度为41的散列桶,每个桶里存放一个列表,分配方式就是根据空闲空间大小除以THRESHOLD得到的商来决定放到哪个桶里
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    // 向页面索引中添加一个页面,这个页面有pgno和freeSpace两个属性.
    // 根据freeSpace的大小来决定插入到哪个位置.
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 该页面属于哪个区间
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 根据所需空间大小，选择一个合适的页面
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 可以占有几个区间
            int number = spaceSize / THRESHOLD;
            // 向上取整
            if(number < INTERVALS_NO) number ++;
            // 从合适的区间开始，找到第一个非空的区间，取出其中的一个页面
            while(number <= INTERVALS_NO) {
                // 该区间没有页面，继续下一个区间
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                // 取出该区间的一个页面
                // 注意这里使用remove方法，将页面从索引中移除，表示该页面正在被使用

                // 注意:
                // 满足要求的页面分配,直接移除,这意味着一个页面无法被并发地读写
                // 使用结束之后,就会再次add到页面索引里面去
                return lists[number].remove(0);
            }
            // 没有找到合适的空闲页面,所以返回null
            return null;
        } finally {
            lock.unlock();
        }
    }

}
