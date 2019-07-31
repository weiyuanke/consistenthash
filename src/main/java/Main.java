import java.util.TreeMap;

public class Main {

    public static void main(String[] args) {
        ConsistentHash consistentHash = new ConsistentHash();
        consistentHash.addServer("server1");
        consistentHash.addServer("server2");
        consistentHash.addServer("server3");
        consistentHash.addServer("server4");

        System.out.println("----");
        System.out.println(consistentHash.getVirtualNodeList());

        System.out.println(consistentHash.getTargetServer("userjack"));
        System.out.println(consistentHash.getTargetServer("usermack"));
        System.out.println(consistentHash.getTargetServer("userjuke"));
        System.out.println(consistentHash.getTargetServer("userbule"));
        System.out.println(consistentHash.getTargetServer("userhull"));
        System.out.println(consistentHash.getTargetServer("useryuku"));
        System.out.println(consistentHash.getTargetServer("uservulu"));
    }
}
