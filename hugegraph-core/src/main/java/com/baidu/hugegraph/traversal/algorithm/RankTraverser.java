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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

public class RankTraverser extends HugeTraverser {

    private final double alpha;

    public RankTraverser(HugeGraph graph, double alpha) {
        super(graph);
        this.alpha = alpha;
    }

    public List<Map<Id, Double>> neighborRank(Id source, List<Step> steps,
                                              long capacity, long limit) {
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");
        checkCapacity(capacity);
        checkLimit(limit);

        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        sources.add(source, new Node(source, null));

        boolean sameLayerTransfer = true;
        int stepNum = steps.size();
        int pathCount = 0;
        long access = 0;
        // Result
        List<Map<Id, Double>> ranks = new ArrayList<>(stepNum + 1);
        ranks.add(ImmutableMap.of(source, 1.0));

        MultivaluedMap<Id, Node> newVertices;
        root : for (Step step : steps) {
            stepNum--;
            newVertices = newMultivalueMap();

            Map<Id, Double> lastLayerRanks = ranks.get(ranks.size() - 1);
            Map<Id, Double> rankIncrs = new HashMap<>();
            MultivaluedMap<Id, Node> adjacencies = newMultivalueMap();
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
                Id curNode = entry.getKey();
                Iterator<Edge> edges = edgesOfVertex(curNode, step.direction,
                                                     step.labels, null,
                                                     step.degree);

                long degree = 0L;
                List<Node> adjacency = new ArrayList<>();
                Set<Id> sameLayerNodes = new HashSet<>();
                Map<Integer, Set<Id>> prevLayerNodes = new HashMap<>();
                neighbor: while (edges.hasNext()) {
                    degree++;
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    // Determine whether it belongs to the same layer
                    if (sources.containsKey(target)) {
                        sameLayerNodes.add(target);
                        continue;
                    }
                    /*
                     * Determine whether it belongs to the previous layers,
                     * if it belongs, update the weight, but no longer pass
                     */
                    for (int i = ranks.size() - 2; i > 0; i--) {
                        Map<Id, Double> prevLayerRanks = ranks.get(i);
                        if (prevLayerRanks.containsKey(target)) {
                            Set<Id> nodes = prevLayerNodes.computeIfAbsent(i,
                                            k -> new HashSet<>());
                            nodes.add(target);
                            continue neighbor;
                        }
                    }

                    for (Node n : entry.getValue()) {
                        // If have loop, skip target
                        if (n.contains(target)) {
                            continue;
                        }
                        Node newNode = new Node(target, n);
                        adjacency.add(newNode);

                        checkCapacity(capacity, ++access, "neighbor ranks");
                    }
                }
                assert degree == sameLayerNodes.size() + prevLayerNodes.size() +
                                 adjacency.size();
                adjacencies.addAll(curNode, adjacency);

                // Add current node's adjacent nodes
                for (Node node : adjacency) {
                    newVertices.add(node.id(), node);
                    // Avoid exceeding limit
                    if (stepNum == 0) {
                        if (limit != NO_LIMIT && ++pathCount >= limit) {
                            break root;
                        }
                    }
                }
                double incrRank = lastLayerRanks.get(curNode) * alpha / degree;
                for (Id node : sameLayerNodes) {
                    // Assign an initial value when firstly update neighbor rank
                    double originRank = rankIncrs.getOrDefault(node, 0.0);
                    rankIncrs.put(node, originRank + incrRank);
                }
                for (Map.Entry<Integer, Set<Id>> e : prevLayerNodes.entrySet()) {
                    Map<Id, Double> prevLayerRanks = ranks.get(e.getKey());
                    for (Id node : e.getValue()) {
                        double originRank = prevLayerRanks.get(node);
                        prevLayerRanks.put(node, originRank + incrRank);
                    }
                }
            }

            Map<Id, Double> newLayerRanks = new LimitOrderedMap(step.backup);
            if (sameLayerTransfer) {
                // First contribute to last layer, then pass to the new layer
                this.contributeLastLayer(rankIncrs, lastLayerRanks);
                this.contributeNewLayer(adjacencies, lastLayerRanks,
                                        newLayerRanks);
            } else {
                // First pass to the new layer, then contribute to last layer
                this.contributeNewLayer(adjacencies, lastLayerRanks,
                                        newLayerRanks);
                this.contributeLastLayer(rankIncrs, lastLayerRanks);
            }
            ranks.add(newLayerRanks);

            // Re-init sources
            sources = newVertices;
        }
        if (stepNum != 0) {
            return ImmutableList.of(ImmutableMap.of());
        }
        return this.cropRanks(ranks, steps);
    }

    private void contributeLastLayer(Map<Id, Double> rankIncrs,
                                     Map<Id, Double> lastLayerRanks) {
        for (Map.Entry<Id, Double> entry : rankIncrs.entrySet()) {
            double originRank = lastLayerRanks.get(entry.getKey());
            double incrRank = entry.getValue();
            lastLayerRanks.put(entry.getKey(), originRank + incrRank);
        }
    }

    private void contributeNewLayer(MultivaluedMap<Id, Node> adjacencies,
                                    Map<Id, Double> lastLayerRanks,
                                    Map<Id, Double> newLayerRanks) {
        for (Map.Entry<Id, List<Node>> entry : adjacencies.entrySet()) {
            Id parentId = entry.getKey();
            long size = entry.getValue().size();
            for (Node node : entry.getValue()) {
                double rank = newLayerRanks.getOrDefault(node.id(), 0.0);
                rank += (lastLayerRanks.get(parentId) * alpha / size);
                newLayerRanks.put(node.id(), rank);
            }
        }
    }

    private List<Map<Id, Double>> cropRanks(List<Map<Id, Double>> ranks,
                                            List<Step> steps) {
        assert ranks.size() > 0;
        List<Map<Id, Double>> results = new ArrayList<>(ranks.size());
        // The first layer is root node
        results.add(ranks.get(0));
        for (int i = 1; i < ranks.size(); i++) {
            Step step = steps.get(i - 1);
            Map<Id, Double> origin = ranks.get(i);
            if (origin.size() > step.number) {
                Map<Id, Double> cropped = topN(origin, step.number);
                results.add(cropped);
            } else {
                results.add(origin);
            }
        }
        return results;
    }

    public static class Step {

        private Directions direction;
        private Map<Id, String> labels;
        private long degree;
        private int number;
        private int backup;

        public Step(Directions direction, Map<Id, String> labels,
                    long degree, int number) {
            this.direction = direction;
            this.labels = labels;
            this.degree = degree;
            this.number = number;
            this.backup = 100000;
        }
    }

    private static class LimitOrderedMap extends TreeMap<Id, Double> {

        private final int capacity;
        private final Map<Id, Double> valueMap;

        public LimitOrderedMap(int count) {
            this(count, DECR_ORDER, new HashMap<>());
        }

        private LimitOrderedMap(int capacity, Ordering<Double> ordering,
                                HashMap<Id, Double> valueMap) {
            super(ordering.onResultOf(Functions.forMap(valueMap))
                          .compound(Ordering.natural()));
            this.capacity = capacity;
            this.valueMap = valueMap;
        }

        @Override
        public Double put(Id k, Double v) {
            if (this.valueMap.containsKey(k)) {
                this.remove(k);
            } else if (this.valueMap.size() >= capacity) {
                Id key = this.lastKey();
                this.remove(key);
                this.valueMap.remove(key);
            }
            this.valueMap.put(k, v);
            return super.put(k, v);
        }

        @Override
        public Double getOrDefault(Object key, Double defaultValue) {
            return this.valueMap.getOrDefault(key, defaultValue);
        }

        @Override
        public boolean containsKey(Object key) {
            return this.valueMap.containsKey(key);
        }
    }

    private static Ordering<Double> DECR_ORDER = Ordering.from((o1, o2) -> {
        if (o1 > o2) {
            return -1;
        } else if (o1 < o2) {
            return 1;
        } else {
            return 0;
        }
    });

    private static Map<Id, Double> topN(Map<Id, Double> origin, int n) {
        E.checkArgument(n > 0, "'N' Must be positive, but got '%s'", n);
        Map<Id, Double> subMap = InsertionOrderUtil.newMap();
        int i = 0;
        for (Map.Entry<Id, Double> entry : origin.entrySet()) {
            subMap.put(entry.getKey(), entry.getValue());
            if (++i >= n) {
                break;
            }
        }
        return subMap;
    }
}
