/**
 * This file is part of core.
 *
 * core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hobbit.core.data;

public class StartCommandData {

    public String image;
    public String type;
    /**
     * Name of the parent container
     */
    public String parent;
    public String[] environmentVariables;
    public String[] networkAliases;

    public StartCommandData(String image, String type, String parent, String[] environmentVariables, String[] netAliases) {
        this.image = image;
        this.type = type;
        this.parent = parent;
        this.environmentVariables = environmentVariables;
        this.networkAliases = netAliases;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public String[] getEnvironmentVariables() {
        return environmentVariables;
    }

    public void setEnvironmentVariables(String[] environmentVariables) {
        this.environmentVariables = environmentVariables;
    }

    public String[] getNetworkAliases() {
        return networkAliases;
    }

    public void setNetworkAliases(String[] networkAliases) {
        this.networkAliases = networkAliases;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StartCommandData [image=");
        builder.append(image);
        builder.append(", type=");
        builder.append(type);
        builder.append(", parent=");
        builder.append(parent);
        builder.append("]");
        return builder.toString();
    }
}
