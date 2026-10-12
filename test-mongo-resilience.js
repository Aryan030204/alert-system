/* -------------------------------------------------------
   Standalone test for utils/mongoResilience.js guard logic.
   Run: node test-mongo-resilience.js
--------------------------------------------------------*/

const assert = require("assert");
const { isGuardActive, armGuard } = require("./utils/mongoResilience");

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("guard is inactive before being armed", () => {
  assert.strictEqual(isGuardActive("perf:ruleA:BAD"), false);
});

test("guard becomes active immediately after arming", () => {
  armGuard("perf:ruleB:BAD", 1000);
  assert.strictEqual(isGuardActive("perf:ruleB:BAD"), true);
});

test("guard does not affect unrelated keys", () => {
  armGuard("perf:ruleC:BAD", 1000);
  assert.strictEqual(isGuardActive("perf:ruleD:BAD"), false);
});

test("guard expires after its duration elapses", async () => {
  armGuard("perf:ruleE:BAD", 50);
  assert.strictEqual(isGuardActive("perf:ruleE:BAD"), true);
  await new Promise((r) => setTimeout(r, 80));
  assert.strictEqual(isGuardActive("perf:ruleE:BAD"), false);
});

test("re-arming extends the window", async () => {
  armGuard("perf:ruleF:BAD", 50);
  await new Promise((r) => setTimeout(r, 30));
  armGuard("perf:ruleF:BAD", 200);
  await new Promise((r) => setTimeout(r, 60));
  assert.strictEqual(isGuardActive("perf:ruleF:BAD"), true);
});

test("duplicate-fire scenario: second call within window is suppressed", () => {
  const key = "perf:ruleG:NEEDS_IMMEDIATE_ATTENTION";
  // first evaluation: not guarded yet -> fires, then arms
  const firstShouldFire = !isGuardActive(key);
  assert.strictEqual(firstShouldFire, true);
  armGuard(key, 3600000);

  // Mongo write for this fire fails silently (simulated) -- state/history
  // never land, so a second event re-reads the same stale state moments later.
  const secondShouldFire = !isGuardActive(key);
  assert.strictEqual(secondShouldFire, false);
});

test("zero/negative duration does not arm the guard", () => {
  armGuard("perf:ruleH:BAD", 0);
  assert.strictEqual(isGuardActive("perf:ruleH:BAD"), false);
  armGuard("perf:ruleH:BAD", -5);
  assert.strictEqual(isGuardActive("perf:ruleH:BAD"), false);
});

(async () => {
  for (const { name, fn } of tests) {
    try {
      await fn();
      console.log(`✅ ${name}`);
    } catch (err) {
      console.error(`❌ ${name}`);
      console.error(err);
      process.exitCode = 1;
    }
  }

  if (process.exitCode) {
    console.error("\nSome tests failed.");
  } else {
    console.log("\nAll mongoResilience guard tests passed.");
  }
})();
