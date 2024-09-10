/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;

import static java.util.Objects.requireNonNull;

@Immutable
public class Column
{
    private final String name;
    private final String type;
    private final ClientTypeSignature typeSignature;
    private boolean isObjectId = false;
    private boolean isObjectOid = false;

    @JsonCreator
    public Column(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type,
            @JsonProperty("typeSignature") ClientTypeSignature typeSignature)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public ClientTypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    public boolean getIsMongoObjectId() {
        return isObjectId;
    }

    public void setIsObjectId(boolean isObjectId) {
        this.isObjectId = isObjectId;
    }

    public boolean getIsObjectOid() {
        return isObjectOid;
    }

    public void setIsObjectOid(boolean isObjectOid) {
        this.isObjectOid = isObjectOid;
    }
}
