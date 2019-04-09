/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.api.traversers;

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.RankTraverser;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/personalrank")
@Singleton
public class PersonalRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                  "edge label '{}', alpha '{}', max depth '{}'",
                  graph, request.source, request.label,
                  request.alpha, request.maxDepth);

        E.checkNotNull(request.source, "source vertex id");
        E.checkNotNull(request.label, "edge label");
        E.checkArgument(request.alpha >= 0.0 && request.alpha <= 1.0,
                        "The alpha must between [0, 1], but got '%s'",
                        request.alpha);
        E.checkArgument(request.maxDepth >= 1,
                        "The max depth must >= 1, but got '%s'",
                        request.maxDepth);

        Id sourceId = VertexAPI.checkAndParseVertexId(request.source);
        HugeGraph g = graph(manager, graph);

        RankTraverser traverser = new RankTraverser(g, request.alpha,
                                                    request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(sourceId, request.label);
        if (request.sorted) {
            ranks = CollectionUtil.sortByValue(ranks, false);
        }
        return JsonUtil.toJson(ranks);
    }

    private static class RankRequest {

        @JsonProperty("source")
        private String source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("max_depth")
        private int maxDepth;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s," +
                                 "alpha=%s,maxDepth=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.maxDepth, this.sorted);
        }
    }
}
