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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ThesisSlotAssigner.Strategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ThesisSlotAssigner}, the thesis fork's placement strategies.
 *
 * <p>What matters here is not that every slice gets a slot — the default assigner already does that
 * — but <em>which TaskManager</em> each slice lands on, since that is the decision Li et al. (IEEE
 * TBD 2023) identify as left to chance in stock Flink. The tests therefore assert on the spread of
 * slices over TaskManagers, plus the determinism the adaptive scheduler's speculative rescale
 * evaluation requires.
 */
class ThesisSlotAssignerTest {

    private static final String TM_A = "tm-a";
    private static final String TM_B = "tm-b";
    private static final String TM_C = "tm-c";

    @AfterEach
    void clearStrategy() {
        System.clearProperty("thesis.assign.strategy");
    }

    /**
     * The Phase-1 observation: with the stock iteration order, both slices of a parallelism-2 job
     * land on the same TaskManager, because the free slots arrive grouped per TM. This is the
     * "before" the strategies are meant to correct.
     */
    @Test
    void defaultStrategyFollowsIterationOrderAndPacksOneTaskManager() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.DEFAULT, 2, freeSlots));

        assertThat(spread).isEqualTo(Collections.singletonMap(TM_A, 2));
    }

    /** The Phase-2 fix: the same job now spreads its two slices over two TaskManagers. */
    @Test
    void roundRobinSpreadsSlicesOverDistinctTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.ROUND_ROBIN, 2, freeSlots));

        assertThat(spread).containsOnlyKeys(TM_A, TM_B).containsValues(1, 1);
    }

    /** With more slices than TaskManagers, round-robin keeps dealing in laps. */
    @Test
    void roundRobinDealsCyclicallyWhenSlicesExceedTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        final List<String> placement = placementOf(assign(Strategy.ROUND_ROBIN, 5, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_C, TM_A, TM_B);
    }

    /**
     * A TaskManager that runs out of free slots drops out of the rotation instead of stalling the
     * assignment, so an unbalanced supply of slots still gets fully used.
     */
    @Test
    void roundRobinSkipsExhaustedTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 1, TM_B, 3);

        final List<String> placement = placementOf(assign(Strategy.ROUND_ROBIN, 4, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_B, TM_B);
    }

    /**
     * Least-loaded reads "loaded" off the free capacity a TaskManager still offers: a TM holding
     * fewer free slots is already busier, so it receives fewer slices.
     */
    @Test
    void leastLoadedPrefersTaskManagerWithMostFreeCapacity() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 3, TM_B, 1);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.LEAST_LOADED, 4, freeSlots));

        assertThat(spread).containsEntry(TM_A, 3).containsEntry(TM_B, 1);
    }

    /** With homogeneous TaskManagers, least-loaded degenerates into round-robin. */
    @Test
    void leastLoadedMatchesRoundRobinOnHomogeneousTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final List<String> placement = placementOf(assign(Strategy.LEAST_LOADED, 4, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_A, TM_B);
    }

    /**
     * The adaptive scheduler calls the assigner speculatively while evaluating candidate rescalings,
     * so the same inputs in a different iteration order must produce the same placement. This is
     * what the internal ordering of slices and slots buys.
     */
    @Test
    void placementIsIndependentOfFreeSlotIterationOrder() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);
        final List<SlotInfo> shuffled = new ArrayList<>(freeSlots);
        Collections.reverse(shuffled);

        for (Strategy strategy : new Strategy[] {Strategy.ROUND_ROBIN, Strategy.LEAST_LOADED}) {
            assertThat(placementOf(assign(strategy, 4, shuffled)))
                    .as("placement under %s must not depend on slot iteration order", strategy)
                    .isEqualTo(placementOf(assign(strategy, 4, freeSlots)));
        }
    }

    /**
     * STOCK must reproduce unpatched Flink, which on a first submission (no previous allocations)
     * means {@link DefaultSlotAssigner} — the same packing the DEFAULT arm produces.
     */
    @Test
    void stockMatchesDefaultSlotAssignerWithoutPreviousAllocations() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        assertThat(placementOf(assign(Strategy.STOCK, 2, freeSlots)))
                .isEqualTo(placementOf(assign(Strategy.DEFAULT, 2, freeSlots)));
    }

    /** STOCK places every slice, like any other arm. */
    @Test
    void stockAssignsEverySlice() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        assertThat(assign(Strategy.STOCK, 4, freeSlots)).hasSize(4);
    }

    /** An unknown strategy name must not take the JobManager down; it falls back to the default. */
    @Test
    void unknownStrategyFallsBackToRoundRobin() {
        System.setProperty("thesis.assign.strategy", "not-a-strategy");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.ROUND_ROBIN);
    }

    @Test
    void strategyIsReadCaseInsensitively() {
        System.setProperty("thesis.assign.strategy", "least_loaded");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.LEAST_LOADED);
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    /** Runs the assigner over a single-vertex job of the given parallelism. */
    private static Collection<SlotAssignment> assign(
            Strategy strategy, int parallelism, Collection<? extends SlotInfo> freeSlots) {
        System.setProperty("thesis.assign.strategy", strategy.name());

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        final JobVertexID vertexId = new JobVertexID();
        final JobInformation jobInformation =
                new TestJobInformation(
                        Collections.singletonList(
                                new TestVertexInformation(
                                        vertexId, parallelism, slotSharingGroup)));

        final Map<JobVertexID, Integer> parallelismForVertices = new HashMap<>();
        parallelismForVertices.put(vertexId, parallelism);

        return new ThesisSlotAssigner()
                .assignSlots(
                        jobInformation,
                        freeSlots,
                        new VertexParallelism(parallelismForVertices),
                        JobAllocationsInformation.empty());
    }

    /** The TaskManager each slice landed on, in slice order. */
    private static List<String> placementOf(Collection<SlotAssignment> assignments) {
        final List<String> placement = new ArrayList<>();
        for (SlotAssignment assignment : assignments) {
            placement.add(taskManagerIdOf(assignment.getSlotInfo()));
        }
        return placement;
    }

    /** How many slices each TaskManager received. */
    private static Map<String, Integer> spreadOf(Collection<SlotAssignment> assignments) {
        final Map<String, Integer> spread = new LinkedHashMap<>();
        for (String taskManager : placementOf(assignments)) {
            spread.merge(taskManager, 1, Integer::sum);
        }
        return spread;
    }

    private static String taskManagerIdOf(SlotInfo slot) {
        return slot.getTaskManagerLocation().getResourceID().getResourceIdString();
    }

    /**
     * Builds free slots from {@code (taskManagerId, slotCount)} pairs, emitted grouped per
     * TaskManager — the order in which the real SlotPool tends to offer them, and the order that
     * makes the default assigner pack a single TM.
     */
    private static List<SlotInfo> slotsOf(Object... taskManagerAndCount) {
        final List<SlotInfo> slots = new ArrayList<>();
        for (int i = 0; i < taskManagerAndCount.length; i += 2) {
            final String taskManagerId = (String) taskManagerAndCount[i];
            final int count = (Integer) taskManagerAndCount[i + 1];
            final TaskManagerLocation location =
                    new TaskManagerLocation(
                            new ResourceID(taskManagerId), InetAddress.getLoopbackAddress(), 42);
            for (int slotNumber = 0; slotNumber < count; slotNumber++) {
                slots.add(new FixedLocationSlotInfo(location, slotNumber));
            }
        }
        return slots;
    }

    /**
     * A {@link SlotInfo} pinned to one TaskManager. {@link TestSlotInfo} cannot be used here: it
     * hands out a freshly generated location on every call, so slots could never be grouped by TM.
     */
    private static final class FixedLocationSlotInfo implements SlotInfo {
        private final AllocationID allocationId = new AllocationID();
        private final TaskManagerLocation taskManagerLocation;
        private final int physicalSlotNumber;

        private FixedLocationSlotInfo(
                TaskManagerLocation taskManagerLocation, int physicalSlotNumber) {
            this.taskManagerLocation = taskManagerLocation;
            this.physicalSlotNumber = physicalSlotNumber;
        }

        @Override
        public AllocationID getAllocationId() {
            return allocationId;
        }

        @Override
        public TaskManagerLocation getTaskManagerLocation() {
            return taskManagerLocation;
        }

        @Override
        public int getPhysicalSlotNumber() {
            return physicalSlotNumber;
        }

        @Override
        public ResourceProfile getResourceProfile() {
            return ResourceProfile.ANY;
        }

        @Override
        public boolean willBeOccupiedIndefinitely() {
            return false;
        }
    }
}
