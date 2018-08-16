package ir.nimbo.searchengine.crawler;

public class Link {
    private String anchorLink;
    private String url;

    public Link(String anchorLink, String url) {
        this.anchorLink = anchorLink;
        this.url = url;
    }
    public Link (String url){
        this.url = url;
    }
    public String getAnchorLink() {
        return anchorLink;
    }

    public String getUrl() {
        return url;
    }

    public void setAnchorLink(String anchorLink) {
        this.anchorLink = anchorLink;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
