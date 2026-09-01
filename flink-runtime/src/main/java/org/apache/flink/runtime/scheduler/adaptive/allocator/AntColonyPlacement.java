/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import java.util.Random;

/**
 * Ant Colony Optimisation over the slice→slot assignment of one rescale.
 *
 * <p>Each ant walks the slices in order and picks a still-free slot with probability proportional to
 * {@code pheromone^ALPHA * heuristic^BETA}; after every colony the best tour so far reinforces its
 * own edges while all pheromone evaporates. The heuristic is a greedy reading of the two cost terms
 * in {@link PlacementInstance}: prefer a TaskManager this ant has loaded least, and prefer a slot
 * that still holds the slice's state.
 *
 * <p><b>The pheromone matrix is local to the call and dies with it.</b> That is not an oversight:
 * the adaptive scheduler invokes the assigner speculatively while evaluating rescalings it may never
 * carry out, so a colony that remembered across calls would have its memory shaped by placements
 * that never happened — and the same question would get different answers depending on how many
 * times it had been asked. All persistence lives in the external learner instead.
 *
 * <p>For the same reason the budget is a fixed iteration count rather than a wall-clock deadline: a
 * time-boxed search returns whatever it reached before the clock ran out, which on a loaded
 * JobManager is a different placement each time. The counts below are sized for the instances this
 * scheduler actually sees (tens of slices, tens of slots), where the whole search is well under a
 * millisecond.
 */
final class AntColonyPlacement {

    private static final int ANTS = 10;
    private static final int ITERATIONS = 20;
    /** Weight of the learned pheromone trail. */
    private static final double ALPHA = 1.0;
    /** Weight of the greedy heuristic; above 1 it dominates early, before any trail exists. */
    private static final double BETA = 2.0;

    private static final double EVAPORATION = 0.1;
    private static final double INITIAL_PHEROMONE = 1.0;

    private AntColonyPlacement() {}

    static int[] search(PlacementInstance instance) {
        final int groups = instance.groupCount();
        final int slots = instance.slotCount();
        final Random random = new Random(instance.seed());

        final double[][] pheromone = new double[groups][slots];
        for (double[] row : pheromone) {
            java.util.Arrays.fill(row, INITIAL_PHEROMONE);
        }

        long maxLocalKeyGroups = 1;
        for (int g = 0; g < groups; g++) {
            for (int s = 0; s < slots; s++) {
                maxLocalKeyGroups = Math.max(maxLocalKeyGroups, instance.localKeyGroups(g, s));
            }
        }

        final Integer[] order = heaviestFirst(instance);

        int[] best = instance.lptPlacement();
        double bestCost = instance.cost(best);

        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            for (int ant = 0; ant < ANTS; ant++) {
                final int[] tour = walk(instance, pheromone, order, maxLocalKeyGroups, random);
                final double cost = instance.cost(tour);
                if (cost < bestCost) {
                    bestCost = cost;
                    best = tour;
                }
            }
            evaporate(pheromone);
            deposit(pheromone, best, bestCost);
        }
        return best;
    }

    /**
     * The order slices are placed in: heaviest first.
     *
     * <p>Index order let the cheap slices claim the fast machines before the expensive ones were
     * even considered, which is how a slice of load ~27 ended up alone on the four-core machine
     * while two of load ~1135 sat on the single-core one (campaign 20260828-174259). Descending
     * load is the order LPT owes its guarantee to, and starting from it means the pheromone
     * refines a sound placement instead of rescuing a bad one.
     */
    private static Integer[] heaviestFirst(PlacementInstance instance) {
        final Integer[] order = new Integer[instance.groupCount()];
        for (int g = 0; g < order.length; g++) {
            order[g] = g;
        }
        java.util.Arrays.sort(
                order,
                (a, b) ->
                        instance.sliceLoad(a) != instance.sliceLoad(b)
                                ? Double.compare(instance.sliceLoad(b), instance.sliceLoad(a))
                                : Integer.compare(a, b));
        return order;
    }

    /** One ant's construction: slices heaviest-first, each taking a slot still free in this tour. */
    private static int[] walk(
            PlacementInstance instance,
            double[][] pheromone,
            Integer[] order,
            long maxLocalKeyGroups,
            Random random) {
        final int slots = instance.slotCount();
        final boolean[] taken = new boolean[slots];
        final double[] loadPerTaskManager = new double[instance.taskManagerCount()];
        final int[] tour = new int[instance.groupCount()];
        final boolean[] placed = new boolean[instance.groupCount()];
        final double[] weights = new double[slots];

        for (int step = 0; step < order.length; step++) {
            final int g = order[step];
            double total = 0;
            for (int s = 0; s < slots; s++) {
                if (taken[s]) {
                    weights[s] = 0;
                    continue;
                }
                final int tm = instance.taskManagerOf(s);

                // How soon this machine would be done with the slice if it took it. Replaces a
                // count of slices per TaskManager, which knew neither how heavy this slice is nor
                // how fast the machine is — the two facts the objective is made of, so the ants
                // were searching blind and the cost function could only rank what they happened to
                // propose. This is the LPT rule used as a construction heuristic, which is what
                // SP-Ant's is too.
                final double finish =
                        (loadPerTaskManager[tm] + instance.sliceLoad(g))
                                / instance.taskManagerSpeed(tm);
                final double compute = 1.0 / (1.0 + finish);

                // Pull towards machines already holding slices this one exchanges data with, so
                // "collect for communication or scatter for compute" is a choice the ants can
                // actually express rather than one only the cost function can score after the fact.
                int partners = 0;
                int partnersHere = 0;
                for (int other = 0; other < placed.length; other++) {
                    if (placed[other] && instance.communicates(g, other)) {
                        partners++;
                        if (instance.taskManagerOf(tour[other]) == tm) {
                            partnersHere++;
                        }
                    }
                }
                final double affinity = partners == 0 ? 0.0 : (double) partnersHere / partners;

                final double locality =
                        (double) instance.localKeyGroups(g, s) / maxLocalKeyGroups;
                final double heuristic = compute + locality + affinity;
                weights[s] = Math.pow(pheromone[g][s], ALPHA) * Math.pow(heuristic, BETA);
                total += weights[s];
            }

            final int chosen = pick(weights, total, taken, random);
            tour[g] = chosen;
            taken[chosen] = true;
            placed[g] = true;
            loadPerTaskManager[instance.taskManagerOf(chosen)] += instance.sliceLoad(g);
        }
        return tour;
    }

    /**
     * Roulette-wheel choice over the free slots. If every weight underflowed to zero the wheel is
     * degenerate, so it falls back to the first free slot rather than leaving the slice unplaced.
     */
    private static int pick(double[] weights, double total, boolean[] taken, Random random) {
        if (total > 0) {
            double threshold = random.nextDouble() * total;
            for (int s = 0; s < weights.length; s++) {
                threshold -= weights[s];
                if (threshold <= 0 && !taken[s]) {
                    return s;
                }
            }
        }
        for (int s = 0; s < taken.length; s++) {
            if (!taken[s]) {
                return s;
            }
        }
        throw new IllegalStateException("No free slot left for an ant to take");
    }

    private static void evaporate(double[][] pheromone) {
        for (double[] row : pheromone) {
            for (int s = 0; s < row.length; s++) {
                row[s] *= (1.0 - EVAPORATION);
            }
        }
    }

    /** Only the best tour reinforces, which keeps the colony from diffusing over a tiny instance. */
    private static void deposit(double[][] pheromone, int[] best, double bestCost) {
        final double strength = 1.0 / (1.0 + bestCost);
        for (int g = 0; g < best.length; g++) {
            pheromone[g][best[g]] += strength;
        }
    }
}
