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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The exact minimiser of {@link PlacementInstance#cost}, by enumerating every assignment of slices
 * to TaskManagers that fits their slot capacities.
 *
 * <p>Exists to make the other arms interpretable. "ACO beats LPT by 9%" says nothing about whether
 * either is any good; "LPT captures 70% of the available gap and ACO 95%" says everything, and needs
 * the optimum to be computable. On this cluster it is: seven slices over machines holding 6, 4 and 2
 * slots is 1148 assignments. The count is roughly (machines)^(slices), so it leaves reach very
 * quickly — hence the cap, and hence heuristics at any realistic scale.
 *
 * <p>Enumerates TaskManagers rather than slots because balance and communication depend only on the
 * machine. Slots within a machine differ for state locality alone, so with a non-zero locality
 * weight this is exact on the other two terms and an approximation on that one.
 */
final class ExhaustivePlacement {

    private static final Logger LOG = LoggerFactory.getLogger(ExhaustivePlacement.class);

    /**
     * Leaves to visit before giving up. A few million costs well under a second and stays far away
     * from the scheduler's own timeouts; beyond that the fallback is the greedy the search was meant
     * to measure, which is reported rather than hidden.
     */
    private static final long MAX_LEAVES = 4_000_000L;

    private ExhaustivePlacement() {}

    static int[] search(PlacementInstance instance) {
        final int groups = instance.groupCount();
        final int machines = instance.taskManagerCount();

        final int[] capacity = new int[machines];
        for (int slot = 0; slot < instance.slotCount(); slot++) {
            capacity[instance.taskManagerOf(slot)]++;
        }
        // Slots of each machine in a stable order, so a machine assignment maps to one placement.
        final int[][] slotsOf = new int[machines][];
        final int[] filled = new int[machines];
        for (int m = 0; m < machines; m++) {
            slotsOf[m] = new int[capacity[m]];
        }
        for (int slot = 0; slot < instance.slotCount(); slot++) {
            final int m = instance.taskManagerOf(slot);
            slotsOf[m][filled[m]++] = slot;
        }

        final int[] best = instance.lptPlacement();
        final double[] bestCost = {instance.cost(best)};
        final int[] used = new int[machines];
        final int[] machineOf = new int[groups];
        final long[] leaves = {0};

        final boolean complete =
                enumerate(instance, 0, groups, machines, capacity, used, machineOf, slotsOf,
                        best, bestCost, leaves);

        LOG.debug(
                "[THESIS_ASSIGN] OPTIMAL visited {} placements, {}, settled on cost {}",
                leaves[0],
                complete ? "exhaustive" : "TRUNCATED - falling back on the best seen",
                bestCost[0]);
        return best;
    }

    /** @return false when the cap stopped the search, so the answer is no longer provably optimal. */
    private static boolean enumerate(
            PlacementInstance instance,
            int group,
            int groups,
            int machines,
            int[] capacity,
            int[] used,
            int[] machineOf,
            int[][] slotsOf,
            int[] best,
            double[] bestCost,
            long[] leaves) {
        if (group == groups) {
            if (++leaves[0] > MAX_LEAVES) {
                return false;
            }
            final int[] candidate = new int[groups];
            final int[] filled = new int[machines];
            for (int g = 0; g < groups; g++) {
                final int m = machineOf[g];
                candidate[g] = slotsOf[m][filled[m]++];
            }
            final double cost = instance.cost(candidate);
            if (cost < bestCost[0]) {
                bestCost[0] = cost;
                System.arraycopy(candidate, 0, best, 0, groups);
            }
            return true;
        }
        for (int m = 0; m < machines; m++) {
            if (used[m] >= capacity[m]) {
                continue;
            }
            used[m]++;
            machineOf[group] = m;
            final boolean complete =
                    enumerate(instance, group + 1, groups, machines, capacity, used, machineOf,
                            slotsOf, best, bestCost, leaves);
            used[m]--;
            if (!complete) {
                return false;
            }
        }
        return true;
    }
}
