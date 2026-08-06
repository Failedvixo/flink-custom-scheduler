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
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
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
    private final List<SlotInfo> slots;
    /** Index of the TaskManager owning each slot, so cost never re-hashes strings. */
    private final int[] taskManagerOfSlot;

    private final int taskManagerCount;
    /** Key groups slice {@code g} would keep locally if placed on slot {@code s}. */
    private final long[][] localKeyGroups;

    private final long totalKeyGroups;
    private final double balanceWeight;
    private final double localityWeight;

    PlacementInstance(
            List<ExecutionSlotSharingGroup> groups,
            List<SlotInfo> slots,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations,
            double balanceWeight,
            double localityWeight) {
        this.groups = groups;
        this.slots = slots;
        this.balanceWeight = balanceWeight;
        this.localityWeight = localityWeight;

        final Map<String, Integer> taskManagerIndex = new HashMap<>();
        this.taskManagerOfSlot = new int[slots.size()];
        for (int s = 0; s < slots.size(); s++) {
            final String taskManager =
                    slots.get(s).getTaskManagerLocation().getResourceID().getResourceIdString();
            taskManagerOfSlot[s] =
                    taskManagerIndex.computeIfAbsent(taskManager, ignored -> taskManagerIndex.size());
        }
        this.taskManagerCount = taskManagerIndex.size();

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

    SlotInfo slot(int index) {
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
        final int[] slicesPerTaskManager = new int[taskManagerCount];
        long retained = 0;
        for (int g = 0; g < slotOfGroup.length; g++) {
            final int slot = slotOfGroup[g];
            slicesPerTaskManager[taskManagerOfSlot[slot]]++;
            retained += localKeyGroups[g][slot];
        }

        final double ideal = (double) slotOfGroup.length / Math.max(1, taskManagerCount);
        double squaredDeviation = 0;
        for (int count : slicesPerTaskManager) {
            squaredDeviation += (count - ideal) * (count - ideal);
        }
        final double imbalance =
                Math.sqrt(squaredDeviation / Math.max(1, taskManagerCount)) / Math.max(1.0, ideal);

        // With no previous allocations every placement retains nothing, so this term is a constant
        // and drops out of the comparison — exactly the right behaviour at a first submission.
        final double lostState =
                totalKeyGroups > 0 ? 1.0 - (double) retained / totalKeyGroups : 0.0;

        return balanceWeight * imbalance + localityWeight * lostState;
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
