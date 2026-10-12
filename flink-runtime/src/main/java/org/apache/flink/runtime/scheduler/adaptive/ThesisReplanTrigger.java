/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * THESIS: a request, from outside the JobManager, to re-apply the placement plan WITHOUT a change
 * of parallelism.
 *
 * <p>WHY (2026-10-12, part B of the thesis). The placement plan at {@code /var/thesis/assignment}
 * is read by {@code ThesisSlotAssigner} only when the execution graph is (re)created, and until now
 * the only thing that recreated it was a rescale. An agent that sees the rate change, or a machine
 * start to struggle, therefore had to wait for the next scheduled rescale — or force one by
 * rescaling down and back up, two restarts for one move. This trigger gives it one restart: the
 * {@link Executing} state polls a request file and, when it sees a new request, goes to {@link
 * Restarting} with the parallelism it already has. Restarting cancels the tasks, returns straight
 * to creating the execution graph (the parallelism did not change), the assigner reads the plan,
 * and the job resumes from its latest completed checkpoint — exactly the path a rescale takes.
 *
 * <p>Flink cannot move one running subtask: a keyed stream job is a single pipelined region, so
 * every task restarts. What the plan controls is which of them change machine, so a plan that moves
 * one slice restarts everything but relocates only that slice.
 *
 * <p>The request file holds one line, {@code <token> [now|checkpoint]}. A token different from the
 * last one seen is a new request; {@code checkpoint} defers it to the next completed checkpoint, so
 * the restart resumes from fresh state and replays as little as possible. The first token seen by a
 * JobManager is adopted without acting on it, so a request left over from a previous run does not
 * restart a newly submitted job.
 *
 * <p>Configured like the assigner's other inputs: {@code THESIS_REPLAN_FILE} / {@code
 * thesis.replan.file} (default {@code /var/thesis/replan}) and {@code THESIS_REPLAN_POLL_MS} /
 * {@code thesis.replan.poll.ms} (default 2000; 0 disables the trigger entirely).
 */
final class ThesisReplanTrigger {

    private static final String ENV_FILE = "THESIS_REPLAN_FILE";
    private static final String PROP_FILE = "thesis.replan.file";
    private static final String DEFAULT_FILE = "/var/thesis/replan";
    private static final String ENV_POLL = "THESIS_REPLAN_POLL_MS";
    private static final String PROP_POLL = "thesis.replan.poll.ms";
    private static final long DEFAULT_POLL_MS = 2000L;

    /** The last token acted on (or adopted), shared by every Executing state of this JobManager. */
    private static final AtomicReference<String> LAST_TOKEN = new AtomicReference<>();

    /** A request read from the file. */
    static final class Request {
        final String token;
        final boolean atCheckpoint;

        Request(String token, boolean atCheckpoint) {
            this.token = token;
            this.atCheckpoint = atCheckpoint;
        }

        @Override
        public String toString() {
            return token + (atCheckpoint ? " (at next checkpoint)" : " (now)");
        }
    }

    private ThesisReplanTrigger() {}

    /** How often to poll, or {@code null} if the trigger is disabled. */
    @Nullable
    static Duration pollInterval() {
        final String raw = setting(PROP_POLL, ENV_POLL);
        long millis = DEFAULT_POLL_MS;
        if (raw != null) {
            try {
                millis = Long.parseLong(raw.trim());
            } catch (NumberFormatException e) {
                millis = DEFAULT_POLL_MS;
            }
        }
        return millis > 0 ? Duration.ofMillis(millis) : null;
    }

    /**
     * A NEW request, or {@code null}. The token is consumed here: the same request is never
     * returned twice, whether the caller acts on it now or defers it to a checkpoint.
     */
    @Nullable
    static Request poll() {
        final Request current = read();
        if (current == null) {
            // No file: remember that nothing was pending, so the FIRST request written after this
            // is acted on instead of being adopted as a leftover.
            LAST_TOKEN.compareAndSet(null, "");
            return null;
        }
        final String previous = LAST_TOKEN.get();
        if (previous == null) {
            // First sight in this JobManager: adopt, do not act (see class comment).
            LAST_TOKEN.compareAndSet(null, current.token);
            return null;
        }
        if (previous.equals(current.token) || !LAST_TOKEN.compareAndSet(previous, current.token)) {
            return null;
        }
        return current;
    }

    @Nullable
    private static Request read() {
        final Path file = Paths.get(firstNonNull(setting(PROP_FILE, ENV_FILE), DEFAULT_FILE));
        if (!Files.isRegularFile(file)) {
            return null;
        }
        final String text;
        try {
            text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            return null;
        }
        if (text.isEmpty()) {
            return null;
        }
        final String[] parts = text.split("\\s+");
        final boolean atCheckpoint = parts.length > 1 && "checkpoint".equalsIgnoreCase(parts[1]);
        return new Request(parts[0], atCheckpoint);
    }

    @Nullable
    private static String setting(String property, String variable) {
        final String configured = System.getProperty(property);
        return configured != null ? configured : System.getenv(variable);
    }

    private static String firstNonNull(@Nullable String value, String fallback) {
        return value != null && !value.trim().isEmpty() ? value.trim() : fallback;
    }
}
