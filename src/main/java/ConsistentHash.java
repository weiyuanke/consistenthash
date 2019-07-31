import java.security.MessageDigest;
import java.util.*;
import java.util.logging.Logger;

public class ConsistentHash {

    private Logger logger = Logger.getLogger(this.getClass().getName());

    /**
     * 节点集合
     */
    private Set<String> serverIdSet;

    /**
     * hash环
     */
    private TreeMap<Integer, String> circle;

    /**
     * 虚拟节点数/实际节点数
     */
    private Integer replicas;

    /**
     * hash函数
     */
    private MessageDigest messageDigest;

    public ConsistentHash() {
        circle = new TreeMap<Integer, String>();
        serverIdSet = new HashSet<String>();
        replicas = 200;
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * 添加一个服务节点
     */
    public synchronized boolean addServer(String serverId) {
        if (serverId == null) {
            return false;
        }

        if (serverIdSet.contains(serverId)) {
            logger.info(serverId + " already existed");
            return false;
        }

        serverIdSet.add(serverId);

        for (int i=0; i<replicas; i++) {
            Integer hashValue = hash(i + "_" + serverId);
            circle.put(hashValue, serverId);
        }

        return true;
    }

    public synchronized boolean removeServer(String serverId) {
        if (serverId == null) {
            return false;
        }

        if (!serverIdSet.contains(serverId)) {
            logger.info(serverId + "not existed");
            return false;
        }

        serverIdSet.remove(serverId);

        for (int i=0; i<replicas; i++) {
            Integer hashValue = hash(serverId + "_" + i);
            circle.remove(hashValue);
        }

        return true;
    }

    /**
     * 根据input，获取对应的服务节点
     */
    public String getTargetServer(String input) {
        Integer inputCode = hash(input);
        Map.Entry<Integer, String> entry = circle.higherEntry(inputCode);
        if (entry == null) {
            return circle.firstEntry().getValue();
        }

        return entry.getValue();
    }

    public Integer getVirtualNodesNumber() {
        return circle.keySet().size();
    }

    public TreeMap<Integer, String> getVirtualNodeList() {
        TreeMap<Integer, String> result = new TreeMap<Integer, String>();

        for (Map.Entry<Integer, String> entry : circle.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private Integer hash(String key) {
        messageDigest.reset();
        messageDigest.update(key.getBytes());

        byte[] digest = messageDigest.digest();

        Integer rlt = 0;
        for (int j=0; j<digest.length/4; j++) {
            Integer result = 0;
            for (int i=0; i<4; i++) {
                result <<= 8;
                result |= ((int) digest[i+j*4]) & 0xFF;
            }
            rlt += result;
        }

        return rlt;
    }
}
