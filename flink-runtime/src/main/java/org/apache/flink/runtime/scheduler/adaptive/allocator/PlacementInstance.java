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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import javax.annotation.Nullable;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The placement problem one call of the thesis assigner has to solve, plus the cost the search
 * arms ({@code ACO}, {@code GA}) minimise over it.
 *
 * <p><b>Why a cost function at all.</b> {@code ROUND_ROBIN} and {@code LEAST_LOADED} optimise a
 * single objective — spread the slices — and on homogeneous TaskManagers they reach the optimum of
 * that objective directly, which is why they are behaviourally identical. A metaheuristic is only
 * worth its cycles when the objective is not solvable by inspection, so the cost here is the genuine
 * trade-off the assigner faces at a rescale:
 *
 * <ul>
 *   <li><b>balance</b> — how unevenly the slices are spread over the TaskManagers; and
 *   <li><b>locality</b> — how much of the key-group state each slice loses by moving off the slot
 *       that already holds it. This is the same quantity {@link StateLocalitySlotAssigner} maximises
 *       (and that {@code ROUND_ROBIN} / {@code LEAST_LOADED} throw away entirely).
 * </ul>
 *
 * Those two pull in opposite directions: perfectly rebalancing a skewed placement means moving state,
 * and keeping all state where it is means accepting the old imbalance. The weights are configurable
 * ({@code THESIS_COST_BALANCE}, {@code THESIS_COST_LOCALITY}) so the trade-off can be moved without
 * recompiling.
 *
 * <p><b>Purity.</b> The instance owns no mutable search state and is rebuilt per call. It also
 * exposes {@link #seed()}: a value derived only from the problem itself (TaskManager ids, physical
 * slot numbers, subtask indices), so the randomised arms are reproducible across the adaptive
 * scheduler's speculative re-invocations. Notably it does <em>not</em> use
 * {@link ExecutionSlotSharingGroup#getId()}, which is a fresh random UUID on every call and would
 * make the "same" instance seed differently each time.
 */
final class PlacementInstance {

    private final List<ExecutionSlotSharingGroup> groups;
    private final List<PhysicalSlot> slots;
    /** Index of the TaskManager owning each slot, so cost never re-hashes strings. */
    private final int[] taskManagerOfSlot;

    private final int taskManagerCount;
    /** Key groups slice {@code g} would keep locally if placed on slot {@code s}. */
    private final long[][] localKeyGroups;

    private final long totalKeyGroups;
    private final double balanceWeight;
    private final double localityWeight;
    /**
     * Relative processing load of each slice, published from outside and defaulting to 1.0.
     *
     * <p>Counting slices per TaskManager is not the same thing as balancing load, and the gap is
     * not academic: measured 2026-08-17, with 4 slices on 3 TaskManagers every placement the arms
     * chose was 2+1+1, so a slice COUNT was identical for all of them and the balance term
     * cancelled out entirely. ACO and GA were then left optimising locality alone, picked the one
     * arrangement that puts both expensive slices on one TaskManager, and lost 45% of throughput
     * doing it. Weighting by measured load is what lets the term distinguish the arrangements it
     * was always meant to distinguish.
     */
    private final double[] sliceLoad;

    /**
     * Relative speed of each TaskManager, indexed like {@link #taskManagerOfSlot}; all 1.0 unless
     * published.
     *
     * <p>Without this the balance term asks every machine to carry an equal SHARE of the load, which
     * is only the right target when the machines are equal. On a heterogeneous cluster the optimum
     * is proportional to capacity, and a cost that does not know capacities will confidently rank a
     * bad placement first. The offline bench puts a number on why this matters: heterogeneity is
     * what opens a 9-14% gap at 4 slices, where identical machines leave 0.0%.
     */
    private final double[] taskManagerSpeed;

    private final double totalSpeed;

    /**
     * Whether balance is scored as makespan rather than as dispersion.
     *
     * <p>They are not the same objective. RMS dispersion (the original, and still the default so
     * earlier campaigns stay comparable) penalises every machine's deviation from its fair share;
     * makespan only cares about the busiest machine, which is what actually bounds throughput and
     * what the offline bench enumerates. Choose makespan when the run is meant to line up with the
     * bench's numbers, dispersion when it is meant to line up with the Phase-2/3 runs.
     */
    private final boolean makespanBalance;

    /**
     * Weight of the communication term, and whether the job has any traffic to model at all.
     *
     * <p>Under slot sharing a slice already holds one subtask of every vertex in its sharing group,
     * so a POINTWISE edge never leaves the slice. Only ALL_TO_ALL edges make slices talk — and there
     * every pair exchanges roughly the same volume, so the only thing a placement can change is HOW
     * MANY PAIRS end up on different TaskManagers. That is the quantity below.
     *
     * <p>It is also what makes the objective worth searching. Capacity alone is separable and
     * concave, so a greedy solves it exactly at any size (verified by enumeration, 0.0% gap across
     * 24 configurations). Counting cross-machine PAIRS is quadratic in the placement and not
     * separable, and a greedy then leaves 10-42% depending on this weight — which is the first
     * provable reason for ACO, GA or a learner to exist here rather than a hoped-for one.
     *
     * <p>Default weight is 0, so every campaign run before this term existed keeps its exact
     * meaning; set {@code THESIS_COST_COMMUNICATION} to switch it on.
     */
    private final double communicationWeight;

    private final boolean hasShuffle;

    PlacementInstance(
            List<ExecutionSlotSharingGroup> groups,
            List<PhysicalSlot> slots,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations,
            double balanceWeight,
            double localityWeight) {
        this(
                groups,
                slots,
                jobInformation,
                previousAllocations,
                balanceWeight,
                localityWeight,
                null,
                null,
                false,
                0.0,
                false);
    }

    PlacementInstance(
            List<ExecutionSlotSharingGroup> groups,
            List<PhysicalSlot> slots,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations,
            double balanceWeight,
            double localityWeight,
            @Nullable double[] sliceLoad,
            @Nullable Map<String, Double> speedByTaskManager,
            boolean makespanBalance,
            double communicationWeight,
            boolean hasShuffle) {
        this.groups = groups;
        this.slots = slots;
        this.balanceWeight = balanceWeight;
        this.localityWeight = localityWeight;
        this.makespanBalance = makespanBalance;
        this.communicationWeight = communicationWeight;
        this.hasShuffle = hasShuffle;

        final Map<String, Integer> taskManagerIndex = new HashMap<>();
        this.taskManagerOfSlot = new int[slots.size()];
        for (int s = 0; s < slots.size(); s++) {
            final String taskManager =
                    slots.get(s).getTaskManagerLocation().getResourceID().getResourceIdString();
            taskManagerOfSlot[s] =
                    taskManagerIndex.computeIfAbsent(taskManager, ignored -> taskManagerIndex.size());
        }
        this.taskManagerCount = taskManagerIndex.size();

        // Keyed by resource id on the way in, by dense index from here on, so cost() never hashes.
        this.taskManagerSpeed = new double[taskManagerCount];
        double speedSum = 0;
        for (Map.Entry<String, Integer> entry : taskManagerIndex.entrySet()) {
            final Double published =
                    speedByTaskManager == null ? null : speedByTaskManager.get(entry.getKey());
            // An unlisted, non-finite or non-positive speed means "nominal", never "infinitely fast".
            final double speed =
                    (published != null
                                    && published > 0
                                    && !Double.isInfinite(published)
                                    && !Double.isNaN(published))
                            ? published
                            : 1.0;
            this.taskManagerSpeed[entry.getValue()] = speed;
            speedSum += speed;
        }
        this.totalSpeed = speedSum;

        final Map<JobVertexID, Integer> parallelism = parallelismOf(groups);
        this.localKeyGroups = new long[groups.size()][slots.size()];
        long total = 0;
        for (int g = 0; g < groups.size(); g++) {
            final Map<AllocationID, Long> scores =
                    localKeyGroupsPerAllocation(
                            groups.get(g), parallelism, jobInformation, previousAllocations);
            for (int s = 0; s < slots.size(); s++) {
                localKeyGroups[g][s] = scores.getOrDefault(slots.get(s).getAllocationId(), 0L);
            }
            total += keyGroupsOf(groups.get(g), parallelism, jobInformation);
        }
        this.totalKeyGroups = total;

        // A null or ill-sized vector means "no load information published", and the cost then
        // behaves exactly as it did before this was added: every slice counts as one.
        this.sliceLoad = new double[groups.size()];
        for (int g = 0; g < groups.size(); g++) {
            final double published = (sliceLoad != null && sliceLoad.length == groups.size())
                    ? sliceLoad[g]
                    : 1.0;
            // A non-finite or negative weight would silently invert the objective.
            this.sliceLoad[g] = (published > 0 && !Double.isInfinite(published)
                            && !Double.isNaN(published))
                    ? published
                    : 1.0;
        }
    }

    double taskManagerSpeed(int taskManagerIndex) {
        return taskManagerSpeed[taskManagerIndex];
    }

    double sliceLoad(int groupIndex) {
        return sliceLoad[groupIndex];
    }

    int groupCount() {
        return groups.size();
    }

    int slotCount() {
        return slots.size();
    }

    ExecutionSlotSharingGroup group(int index) {
        return groups.get(index);
    }

    PhysicalSlot slot(int index) {
        return slots.get(index);
    }

    int taskManagerOf(int slotIndex) {
        return taskManagerOfSlot[slotIndex];
    }

    int taskManagerCount() {
        return taskManagerCount;
    }

    long localKeyGroups(int groupIndex, int slotIndex) {
        return localKeyGroups[groupIndex][slotIndex];
    }

    /**
     * Cost of a candidate placement, where {@code slotOfGroup[g]} is the slot index slice {@code g}
     * is placed on. Lower is better; both terms are normalised to roughly [0, 1] so the weights mean
     * what they look like they mean.
     */
    double cost(int[] slotOfGroup) {
        final double[] loadPerTaskManager = new double[taskManagerCount];
        long retained = 0;
        double totalLoad = 0;
        for (int g = 0; g < slotOfGroup.length; g++) {
            final int slot = slotOfGroup[g];
            loadPerTaskManager[taskManagerOfSlot[slot]] += sliceLoad[g];
            totalLoad += sliceLoad[g];
            retained += localKeyGroups[g][slot];
        }

        // The load a perfectly divisible schedule would put on each machine: proportional to its
        // speed, which collapses to an equal share exactly when the speeds are equal.
        final double meanIdeal = totalLoad / Math.max(1, taskManagerCount);
        final double imbalance;
        if (makespanBalance) {
            // How much longer the busiest machine runs than the perfectly divisible schedule would.
            // Zero for a perfect split, and comparable across instances of different total load.
            double makespan = 0;
            for (int t = 0; t < taskManagerCount; t++) {
                makespan = Math.max(makespan, loadPerTaskManager[t] / taskManagerSpeed[t]);
            }
            final double idealMakespan = totalLoad / Math.max(1e-9, totalSpeed);
            imbalance = makespan / Math.max(1e-9, idealMakespan) - 1.0;
        } else {
            double squaredDeviation = 0;
            for (int t = 0; t < taskManagerCount; t++) {
                final double ideal = totalLoad * taskManagerSpeed[t] / Math.max(1e-9, totalSpeed);
                squaredDeviation += (loadPerTaskManager[t] - ideal) * (loadPerTaskManager[t] - ideal);
            }
            // Normalised by the mean share rather than by a constant so the term stays comparable
            // when the published loads change scale; with equal speeds and unit loads this reduces
            // to the previous formula exactly.
            imbalance =
                    Math.sqrt(squaredDeviation / Math.max(1, taskManagerCount))
                            / Math.max(1e-9, meanIdeal);
        }

        // With no previous allocations every placement retains nothing, so this term is a constant
        // and drops out of the comparison — exactly the right behaviour at a first submission.
        final double lostState =
                totalKeyGroups > 0 ? 1.0 - (double) retained / totalKeyGroups : 0.0;

        return balanceWeight * imbalance
                + localityWeight * lostState
                + communicationWeight * crossMachineTraffic(slotOfGroup);
    }

    /**
     * Fraction of slice PAIRS that sit on different TaskManagers, i.e. whose shuffle traffic has to
     * cross a Netty connection instead of staying in a local channel.
     *
     * <p>Zero when the job has no all-to-all edge — there is then nothing between slices to pay for —
     * and zero when every slice shares one machine. Normalised by the total number of pairs so the
     * term lands in [0, 1] like the other two and the weights mean what they look like they mean.
     */
    private double crossMachineTraffic(int[] slotOfGroup) {
        if (!hasShuffle || communicationWeight == 0.0 || slotOfGroup.length < 2) {
            return 0.0;
        }
        int cross = 0;
        int total = 0;
        for (int a = 0; a < slotOfGroup.length; a++) {
            for (int b = a + 1; b < slotOfGroup.length; b++) {
                total++;
                if (taskManagerOfSlot[slotOfGroup[a]] != taskManagerOfSlot[slotOfGroup[b]]) {
                    cross++;
                }
            }
        }
        return total == 0 ? 0.0 : (double) cross / total;
    }

    /**
     * A seed that depends only on the problem instance, so a randomised arm returns the same
     * placement every time the scheduler asks the same question — including the speculative asks it
     * makes while evaluating a rescale it may not even carry out.
     */
    long seed() {
        long seed = 1125899906842597L; // an odd prime, the usual string-hash starting point
        for (int s = 0; s < slots.size(); s++) {
            seed = 31 * seed + taskManagerOfSlot[s];
            seed = 31 * seed + slots.get(s).getPhysicalSlotNumber();
        }
        for (ExecutionSlotSharingGroup group : groups) {
            for (ExecutionVertexID vertex : group.getContainedExecutionVertices()) {
                seed = 31 * seed + vertex.getSubtaskIndex();
                seed = 31 * seed + vertex.getJobVertexId().hashCode();
            }
        }
        return seed;
    }

    // ------------------------------------------------------------------------
    // state locality, computed the way StateLocalitySlotAssigner computes it
    // ------------------------------------------------------------------------

    /**
     * How many key groups the slice would find already local on each previously used allocation.
     * Reimplemented rather than delegated because {@code StateLocalitySlotAssigner}'s score type is
     * private to that class; the key-group arithmetic below is deliberately identical to it.
     */
    private static Map<AllocationID, Long> localKeyGroupsPerAllocation(
            ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations) {
        final Map<AllocationID, Long> scores = new HashMap<>();
        for (ExecutionVertexID vertex : group.getContainedExecutionVertices()) {
            final KeyGroupRange range = keyGroupRangeOf(vertex, parallelism, jobInformation);
            for (VertexAllocationInformation allocation :
                    previousAllocations.getAllocations(vertex.getJobVertexId())) {
                final long shared =
                        allocation.getKeyGroupRange().getIntersection(range).getNumberOfKeyGroups();
                if (shared > 0) {
                    scores.merge(allocation.getAllocationID(), shared, Long::sum);
                }
            }
        }
        return scores;
    }

    private static long keyGroupsOf(
            ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation) {
        long total = 0;
        for (ExecutionVertexID vertex : group.getContainedExecutionVertices()) {
            total += keyGroupRangeOf(vertex, parallelism, jobInformation).getNumberOfKeyGroups();
        }
        return total;
    }

    private static KeyGroupRange keyGroupRangeOf(
            ExecutionVertexID vertex,
            Map<JobVertexID, Integer> parallelism,
            JobInformation jobInformation) {
        return KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                jobInformation.getVertexInformation(vertex.getJobVertexId()).getMaxParallelism(),
                parallelism.get(vertex.getJobVertexId()),
                vertex.getSubtaskIndex());
    }

    private static Map<JobVertexID, Integer> parallelismOf(List<ExecutionSlotSharingGroup> groups) {
        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        for (ExecutionSlotSharingGroup group : groups) {
            for (ExecutionVertexID vertex : group.getContainedExecutionVertices()) {
                parallelism.merge(vertex.getJobVertexId(), 1, Integer::sum);
            }
        }
        return parallelism;
    }

    /** The trivial placement — slice {@code i} on slot {@code i} — used as a search starting point. */
    int[] identityPlacement() {
        final int[] placement = new int[groups.size()];
        for (int g = 0; g < placement.length; g++) {
            placement[g] = g;
        }
        return placement;
    }

    /** The slot indices a placement may draw from, in their stable order. */
    List<Integer> slotIndices() {
        final List<Integer> indices = new ArrayList<>(slots.size());
        for (int s = 0; s < slots.size(); s++) {
            indices.add(s);
        }
        return indices;
    }
}
