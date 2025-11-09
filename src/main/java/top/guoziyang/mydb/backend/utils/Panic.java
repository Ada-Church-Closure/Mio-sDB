package top.guoziyang.mydb.backend.utils;

public class Panic {
    /**
     * 一旦发生异常,打印异常信息并退出程序
     * @param err 发生的异常
     */
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
