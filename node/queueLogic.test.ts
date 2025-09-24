import { describe, expect, it } from "vitest";
import { summarizeStreamEntries, shouldProcessBatch } from "./queueLogic.js";

describe("shouldProcessBatch", () => {
  it("returns false when below threshold and producer still running", () => {
    expect(shouldProcessBatch({ streamLength: 3, threshold: 5, producerFinished: false })).toBe(false);
  });

  it("returns true once threshold is met", () => {
    expect(shouldProcessBatch({ streamLength: 5, threshold: 5, producerFinished: false })).toBe(true);
  });

  it("returns true when producer finished but tasks remain", () => {
    expect(shouldProcessBatch({ streamLength: 1, threshold: 5, producerFinished: true })).toBe(true);
  });

  it("returns false when there is nothing left to process", () => {
    expect(shouldProcessBatch({ streamLength: 0, threshold: 5, producerFinished: true })).toBe(false);
  });
});

describe("summarizeStreamEntries", () => {
  it("collects tasks and acknowledgement ids", () => {
    const summary = summarizeStreamEntries(
      {
        "2-0": [["task", "task-2"], ["meta", "ignored"]],
        "1-0": [["task", "task-1"]],
      },
      { taskField: "task" }
    );

    expect(summary).toEqual({
      ackIds: ["1-0", "2-0"],
      tasks: ["task-1", "task-2"],
    });
  });

  it("ignores entries that do not contain the task field but still acknowledges them", () => {
    const summary = summarizeStreamEntries(
      {
        "3-0": [["note", "value"]],
        "4-0": null,
      },
      { taskField: "task" }
    );

    expect(summary).toEqual({
      ackIds: ["3-0", "4-0"],
      tasks: [],
    });
  });
});
