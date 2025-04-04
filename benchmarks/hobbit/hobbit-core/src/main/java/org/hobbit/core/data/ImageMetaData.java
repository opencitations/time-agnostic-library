package org.hobbit.core.data;

import java.util.Date;
import java.util.Set;

import org.apache.jena.rdf.model.Model;

public class ImageMetaData implements Cloneable {

    public String uri;
    public String name;
    public String description;
    public String mainImage;
    public Set<String> usedImages;
    public transient Model rdfModel;
    public String source;
    public Date date;
    public String defError;
    
    public ImageMetaData() {
    }
    
    public ImageMetaData(ImageMetaData other) {
        this.uri = other.uri;
        this.name = other.name;
        this.description = other.description;
        this.mainImage = other.mainImage;
        this.usedImages = other.usedImages;
        this.rdfModel = other.rdfModel;
        this.source = other.source;
        this.date = other.date;
        this.defError = other.defError;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ImageMetaData other = (ImageMetaData) obj;
        if (date == null) {
            if (other.date != null)
                return false;
        } else if (!date.equals(other.date))
            return false;
        if (source == null) {
            if (other.source != null)
                return false;
        } else if (!source.equals(other.source))
            return false;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!uri.equals(other.uri))
            return false;
        return true;
    }

    /**
     * @return the uri
     */
    public String getUri() {
        return uri;
    }

    /**
     * @param uri the uri to set
     */
    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return the mainImage
     */
    public String getMainImage() {
        return mainImage;
    }

    /**
     * @param mainImage the mainImage to set
     */
    public void setMainImage(String mainImage) {
        this.mainImage = mainImage;
    }

    /**
     * @return the usedImages
     */
    public Set<String> getUsedImages() {
        return usedImages;
    }

    /**
     * @param usedImages the usedImages to set
     */
    public void setUsedImages(Set<String> usedImages) {
        this.usedImages = usedImages;
    }

    /**
     * @return the rdfModel
     */
    public Model getRdfModel() {
        return rdfModel;
    }

    /**
     * @param rdfModel the rdfModel to set
     */
    public void setRdfModel(Model rdfModel) {
        this.rdfModel = rdfModel;
    }

    /**
     * @return the source
     */
    public String getSource() {
        return source;
    }

    /**
     * @param source the source to set
     */
    public void setSource(String source) {
        this.source = source;
    }

    /**
     * @return the date
     */
    public Date getDate() {
        return date;
    }

    /**
     * @param date the date to set
     */
    public void setDate(Date date) {
        this.date = date;
    }

    /**
     * @return the defError
     */
    public String getDefError() {
        return defError;
    }

    /**
     * @param defError the defError to set
     */
    public void setDefError(String defError) {
        this.defError = defError;
    }
    
    @Override
    public Object clone() {
        return new ImageMetaData(this);
    }
}
