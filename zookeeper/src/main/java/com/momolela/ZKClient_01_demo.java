package com.momolela;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZKClient_01_demo {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 创建一个zk客户端
        ZooKeeper zk = new ZooKeeper("10.10.2.60:2181,10.10.2.61:2181,10.10.2.62:2181", 30000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getState());
                System.out.println(watchedEvent.getType());
                System.out.println(watchedEvent.getPath());
            }
        });

        // 创建一个持久有序的节点suntest，内容为haha
        zk.create("/suntest", "haha".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

        // 获取数据开启监听
        zk.getData("/suntest", true, null);

        // 给节点设置新的值为hehe，version设置为-1，说明version交由zk自己维护。这个执行的时候会触发zk客户端的Watcher，打印 SyncConnected NodeDataChanged /suntest
        zk.setData("/suntest", "hehe".getBytes(), -1);

        // 再次设置新的值，不再触发zk客户端的Watcher事件
        zk.setData("/suntest", "xixi".getBytes(), -1);

        // 创建子目录节点
        zk.create("/suntest/suntestChild_1", "hahaChild_1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/suntest/suntestChild_2", "hahaChild_2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // 取出子节点目录列表
        zk.getChildren("/suntest", true);

        // 修改子目录节点数据
        zk.setData("/suntest/suntestChild_1", "suntestChild_1_change".getBytes(), -1);

        // 删除子目录节点
        zk.delete("/suntest/suntestChild_2", -1);

        // 判读子目录节点是否存在 exist  not exist
        Stat stat = zk.exists("/suntest/suntestChild_2", false);

        // 关闭连接
        zk.close();
    }
}
