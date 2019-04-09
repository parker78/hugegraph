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
import java.util.function.BiFunction;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

public class RankTraverser extends HugeTraverser {

    private final double alpha;
    private final long maxDepth;

    public RankTraverser(HugeGraph graph, double alpha) {
        super(graph);
        this.alpha = alpha;
        this.maxDepth = 0L;
    }

    public RankTraverser(HugeGraph graph, double alpha, long maxDepth) {
        super(graph);
        this.alpha = alpha;
        this.maxDepth = maxDepth;
    }

    public Map<Id, Double> personalRank(Id source, String label) {
        Id labelId = this.graph().edgeLabel(label).id();
        Directions dir = this.getStartDirection(source, label);
        long degree = this.degreeOfVertex(source, dir, labelId);
        if (degree < 1) {
            return ImmutableMap.of(source, 1.0);
        }

        long depth = 0;
        Set<Id> outSeeds = new HashSet<>();
        Set<Id> inSeeds = new HashSet<>();
        if (dir == Directions.OUT) {
            outSeeds.add(source);
        } else {
            inSeeds.add(source);
        }
        Map<Id, Double> ranks = new HashMap<>();
        ranks.put(source, 1.0);
        while (++depth <= this.maxDepth) {
            Map<Id, Double> incrRanks = this.getIncrRanks(outSeeds, inSeeds,
                                                          labelId, ranks);
            ranks = this.compensateSourceVertex(source, incrRanks);
        }
        return ranks;
    }

    private Map<Id, Double> compensateSourceVertex(Id source,
                                                   Map<Id, Double> incrRanks) {
        double sourceRank = incrRanks.getOrDefault(source, 0.0);
        sourceRank += (1 - this.alpha);
        incrRanks.put(source, sourceRank);
        return incrRanks;
    }

//    public Map<Id, Double> neighborRankBak(Id source, List<Step> steps) {
//        Set<Id> seeds = new HashSet<>();
//        seeds.add(source);
//        Map<Id, Double> ranks = new HashMap<>();
//        ranks.put(source, 1.0);
//        for (int depth = 0; depth < this.maxDepth; depth++) {
//            Step step = steps.get(depth);
//            Id label = null;
//            if (step.label != null) {
//                label = this.graph().edgeLabel(step.label).id();
//            }
//            Directions direction = step.direction;
//
//            Map<Id, Double> incrRanks = this.getIncrRanks(seeds, direction,
//                                                          label, ranks);
//            this.combineIncrement(ranks, incrRanks);
//            seeds = incrRanks.keySet();
//        }
//        return ranks;
//    }

    private void combineIncrement(Map<Id, Double> ranks,
                                  Map<Id, Double> incrRanks) {
        // Update the rank of the neighbor vertices
        for (Map.Entry<Id, Double> entry : incrRanks.entrySet()) {
            double oldRank = ranks.getOrDefault(entry.getKey(), 0.0);
            double incrRank = entry.getValue();
            double newRank = oldRank + incrRank;
            ranks.put(entry.getKey(), newRank);
        }
    }

    private Map<Id, Double> getIncrRanks(Set<Id> outSeeds, Set<Id> inSeeds,
                                         Id label, Map<Id, Double> ranks) {
        Map<Id, Double> incrRanks = new HashMap<>();
        BiFunction<Set<Id>, Directions, Set<Id>> neighborIncrRanks = (seeds, dir) -> {
            Set<Id> tmpSeeds = new HashSet<>();
            for (Id seed : seeds) {
                long degree = this.degreeOfVertex(seed, dir, label);
                assert degree > 0;
                // Must be exist
                double originRank = ranks.get(seed);
                double spreadRank = originRank * alpha / degree;

                Set<Id> neighbors = this.adjacentVertices(seed, dir, label);
                // Collect all neighbors increment
                for (Id neighbor : neighbors) {
                    tmpSeeds.add(neighbor);
                    // Assign an initial value when firstly update neighbor rank
                    double incrRank = incrRanks.getOrDefault(neighbor, 0.0);
                    incrRank += spreadRank;
                    incrRanks.put(neighbor, incrRank);
                }
            }
            return tmpSeeds;
        };

        Set<Id> tmpInSeeds = neighborIncrRanks.apply(outSeeds, Directions.OUT);
        Set<Id> tmpOutSeeds = neighborIncrRanks.apply(inSeeds, Directions.IN);

        outSeeds.addAll(tmpOutSeeds);
        inSeeds.addAll(tmpInSeeds);
        return incrRanks;
    }

    private Map<Id, Double> getIncrRanks(Set<Id> seeds, Directions dir,
                                         Id label, Map<Id, Double> ranks) {
        Map<Id, Double> rankIncrs = new HashMap<>();
        for (Id seed : seeds) {
            long degree = this.degreeOfVertex(seed, dir, label);
            assert degree > 0;
            // Must be exist
            double originRank = ranks.get(seed);
            double spreadRank = originRank * alpha / degree;

            Set<Id> neighbors = this.adjacentVertices(seed, dir, label);
            // Collect all neighbors increment
            for (Id neighbor : neighbors) {
                // Assign an initial value when firstly update neighbor rank
                double incrRank = rankIncrs.getOrDefault(neighbor, 0.0);
                incrRank += spreadRank;
                rankIncrs.put(neighbor, incrRank);
            }
        }
        return rankIncrs;
    }

    private Directions getStartDirection(Id source, String label) {
        // NOTE: The outer layer needs to ensure that the vertex Id is valid
        HugeVertex vertex = (HugeVertex) graph().vertices(source).next();
        VertexLabel vertexLabel = vertex.schemaLabel();
        EdgeLabel edgeLabel = this.graph().edgeLabel(label);
        E.checkArgument(edgeLabel.linkWithLabel(vertexLabel.id()),
                        "The vertex '%s' doesn't link with edge label '%s'",
                        source, label);

        if (edgeLabel.sourceLabel().equals(vertexLabel.id())) {
            return Directions.OUT;
        } else {
            assert edgeLabel.targetLabel().equals(vertexLabel.id());
            return Directions.IN;
        }
    }

    public List<Map<Id, Double>> neighborRank(Id source, List<Step> steps,
                                              long capacity, long limit) {
        E.checkArgument(!steps.isEmpty(), "The steps can't be empty");
        checkCapacity(capacity);
        checkLimit(limit);

        boolean sameLayerTransfer = false;

        MultivaluedMap<Id, Node> sources = newMultivalueMap();
        sources.add(source, new Node(source, null));

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
            Iterator<Edge> edges;

            Map<Id, Double> lastLayerRanks = ranks.get(ranks.size() - 1);
            Map<Id, Double> rankIncrs = new HashMap<>();
            MultivaluedMap<Id, Node> adjacencies = newMultivalueMap();
            // Traversal vertices of previous level
            for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
                List<Node> adjacency = new ArrayList<>();
                edges = edgesOfVertex(entry.getKey(), step.direction,
                                      step.labels, step.properties,
                                      step.degree);
                long degree = 0L;
                Set<Id> sameLayerNodes = new HashSet<>();
                Map<Integer, Set<Id>> prevLayerNodes = new HashMap<>();
                neighbor: while (edges.hasNext()) {
                    degree++;
                    HugeEdge edge = (HugeEdge) edges.next();
                    Id target = edge.id().otherVertexId();
                    // Determine whether it belongs to the same layer
                    if (sources.keySet().contains(target)) {
                        sameLayerNodes.add(target);
                        continue;
                    }
                    /*
                     * Determine whether it belongs to the previous L layer,
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
                assert degree == sameLayerNodes.size() + adjacency.size();
                adjacencies.addAll(entry.getKey(), adjacency);

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

                for (Id sameLayerNode : sameLayerNodes) {
                    // Assign an initial value when firstly update neighbor rank
                    double incrRank = rankIncrs.getOrDefault(sameLayerNode, 0.0);
                    incrRank += (lastLayerRanks.get(entry.getKey()) * alpha / degree);
                    rankIncrs.put(sameLayerNode, incrRank);
                }
                for (Map.Entry<Integer, Set<Id>> e : prevLayerNodes.entrySet()) {
                    int layer = e.getKey();
                    Map<Id, Double> prevLayerRanks = ranks.get(layer);
                    for (Id id : e.getValue()) {
                        double originRank = prevLayerRanks.get(id);
                        double incrRank = lastLayerRanks.get(entry.getKey()) * alpha / degree;
                        prevLayerRanks.put(id, originRank + incrRank);
                    }
                }
            }

            Map<Id, Double> newLayerRanks = new LimitValueOrderedMap(step.backup);
            if (sameLayerTransfer) {
                // First contribute to last layer, then pass to the new layer
                for (Map.Entry<Id, Double> entry : rankIncrs.entrySet()) {
                    double originRank = lastLayerRanks.get(entry.getKey());
                    double incrRank = entry.getValue();
                    lastLayerRanks.put(entry.getKey(), originRank + incrRank);
                }
                for (Map.Entry<Id, List<Node>> entry : adjacencies.entrySet()) {
                    Id parentId = entry.getKey();
                    long size = entry.getValue().size();
                    for (Node node : entry.getValue()) {
                        double rank = newLayerRanks.getOrDefault(node.id(), 0.0);
                        rank += (lastLayerRanks.get(parentId) * alpha / size);
                        newLayerRanks.put(node.id(), rank);
                    }
                }
            } else {
                // First pass to the new layer, then contribute to last layer
                for (Map.Entry<Id, List<Node>> entry : adjacencies.entrySet()) {
                    Id parentId = entry.getKey();
                    long size = entry.getValue().size();
                    for (Node node : entry.getValue()) {
                        double rank = newLayerRanks.getOrDefault(node.id(), 0.0);
                        rank += (lastLayerRanks.get(parentId) * alpha / size);
                        newLayerRanks.put(node.id(), rank);
                    }
                }
                for (Map.Entry<Id, Double> entry : rankIncrs.entrySet()) {
                    double originRank = lastLayerRanks.get(entry.getKey());
                    double incrRank = entry.getValue();
                    lastLayerRanks.put(entry.getKey(), originRank + incrRank);
                }
            }



            ranks.add(newLayerRanks);
            // Re-init sources
            sources = newVertices;
        }
        if (stepNum != 0) {
            return ImmutableList.of(ImmutableMap.of());
        }
        for (int i = 1; i < ranks.size(); i++) {
            Step step = steps.get(i - 1);
            Map<Id, Double> rank = ranks.get(i);
            if (rank.size() > step.number) {
                rank = topN(rank, step.number);
                ranks.add(i, rank);
            }
        }
        return ranks;
    }

    public static class Step {

        private Directions direction;
        private Map<Id, String> labels;
        private Map<String, Object> properties;
        private long degree;
        private int number;
        private int backup;

        public Step(Directions direction, Map<Id, String> labels,
                    Map<String, Object> properties, long degree, int number) {
            this.direction = direction;
            this.labels = labels;
            this.properties = properties;
            this.degree = degree;
            this.number = number;
            this.backup = 100000;
        }
    }

    private static class LimitValueOrderedMap extends TreeMap<Id, Double> {

        private static Ordering<Double> DESC = Ordering.from((o1, o2) -> {
            if (o1 > o2) {
                return -1;
            } else if (o1 < o2) {
                return 1;
            } else {
                return 0;
            }
        });

        private final int count;
        private final Map<Id, Double> valueMap;

        public LimitValueOrderedMap(int count) {
            this(count, DESC, new HashMap<>());
        }

        private LimitValueOrderedMap(int count, Ordering<Double> ordering,
                                     HashMap<Id, Double> valueMap) {
            super(ordering.onResultOf(Functions.forMap(valueMap))
                          .compound(Ordering.natural()));
            this.count = count;
            this.valueMap = valueMap;
        }

        @Override
        public Double put(Id k, Double v) {
            if (this.valueMap.containsKey(k)) {
                remove(k);
            } else if (this.valueMap.size() >= count) {
                E.checkNotNull(remove(lastKey()), "value");
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
