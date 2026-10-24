import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";

describe("OpenCodeHarness in several objects at once", () => {
  it.each(["root", "own"])(
    "answers prompts made to five objects at once (%s session)",
    async (sessions) => {
      const stubs = Array.from({ length: 5 }, () =>
        env.OPENCODE_HARNESS_TEST.getByName(crypto.randomUUID())
      );
      const ids = await Promise.all(
        stubs.map((stub) =>
          sessions === "own" ? stub.createSession() : undefined
        )
      );
      for (let round = 0; round < 5; round++) {
        const results = await Promise.all(
          stubs.map((stub, i) => stub.prompt(`hello ${round}`, ids[i]))
        );
        expect(results.map((result) => result.status)).toEqual(
          stubs.map(() => "done")
        );
      }
    }
  );
});

describe("timers in several objects at once", () => {
  it.each(["immediate", "timeout"] as const)("%s", async (kind) => {
    const stubs = Array.from({ length: 5 }, () =>
      env.OPENCODE_HARNESS_TEST.getByName(crypto.randomUUID())
    );
    const cross = await Promise.all(
      stubs.map((stub) => stub.yieldProbe(kind, 200))
    );
    console.log(`[yield] ${kind} cross=${cross.join(",")}`);
    expect(cross).toEqual([0, 0, 0, 0, 0]);
  });
});

describe("a module-level Effect semaphore in several objects at once", () => {
  it("resumes each waiter in an I/O context it can use", async () => {
    const stubs = Array.from({ length: 2 }, () =>
      env.OPENCODE_HARNESS_TEST.getByName(crypto.randomUUID())
    );
    const usable = await Promise.all(
      stubs.map((stub) => stub.semaphoreProbe(50))
    );
    console.log(`[trigger] own SQL usable after acquire: ${usable.join(",")}`);
    expect(usable).toEqual([true, true]);
  });
});
