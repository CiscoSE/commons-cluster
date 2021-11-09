package com.cisco.commons.cluster.controller;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Cluster member representation.
 * 
 * @author Liran Mendelovich
 * 
 * Copyright 2021 Cisco Systems
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
@Data
@AllArgsConstructor
public class ClusterMember {

    private String serviceName;
    private String memberName;
    private String address;
    private String fullPath;
    private boolean isLeader;

    /**
     * Get IP address - result is not reliable, therefore deprecated.
     * @return
     */
    @Deprecated
    public String getAddress() {
        return address;
    }

}
