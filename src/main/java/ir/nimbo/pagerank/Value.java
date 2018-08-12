package ir.nimbo.pagerank;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class Value implements Serializable {
    Double pageRank;
    Set<String> outLinks;

    Value(Set<String> outLinks, Double pageRank) {
        this.pageRank = pageRank;
        this.outLinks = outLinks;
    }

    @Override
    public String toString() {
        return pageRank.toString();
    }
}
