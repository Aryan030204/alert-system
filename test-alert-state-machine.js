/* -------------------------------------------------------
   Pure unit test for the discount/COD state-transition logic added to
   alertEngine.js: classifyState() (the "higher is worse" NORMAL/
   TRIGGERED/CRITICAL classifier), hasStateChanged() (the existing
   allowed-transition gate, reused as-is), and discountOutpaceSeverity()
   (the two-threshold -> single-ratio derivation for
   DISCOUNT_OUTPACING_SALES). No DB, no network.
   Run: node test-alert-state-machine.js
--------------------------------------------------------*/

require("dotenv").config({ path: "./.env" });

const assert = require("assert");
const {
  classifyState,
  hasStateChanged,
  discountOutpaceSeverity,
  CRITICAL_THRESHOLD_MULTIPLIER,
} = require("./alertEngine");

const tests = [];
function test(name, fn) {
  tests.push({ name, fn });
}

test("CRITICAL_THRESHOLD_MULTIPLIER is 2", () => {
  assert.strictEqual(CRITICAL_THRESHOLD_MULTIPLIER, 2);
});

// -- classifyState: value vs threshold*1 / threshold*multiplier -------------

test("classifyState: below threshold is NORMAL", () => {
  assert.strictEqual(classifyState(5, 10), "NORMAL");
});

test("classifyState: at threshold is NORMAL (strictly greater required)", () => {
  assert.strictEqual(classifyState(10, 10), "NORMAL");
});

test("classifyState: just above threshold is TRIGGERED", () => {
  assert.strictEqual(classifyState(10.01, 10), "TRIGGERED");
});

test("classifyState: at critical boundary (2x) is TRIGGERED, not CRITICAL", () => {
  assert.strictEqual(classifyState(20, 10), "TRIGGERED");
});

test("classifyState: just above critical boundary is CRITICAL", () => {
  assert.strictEqual(classifyState(20.01, 10), "CRITICAL");
});

test("classifyState: null/undefined value is NORMAL (fail safe, not open)", () => {
  assert.strictEqual(classifyState(null, 10), "NORMAL");
  assert.strictEqual(classifyState(undefined, 10), "NORMAL");
});

test("classifyState: null threshold is NORMAL", () => {
  assert.strictEqual(classifyState(100, null), "NORMAL");
});

test("classifyState: NaN value is NORMAL", () => {
  assert.strictEqual(classifyState(NaN, 10), "NORMAL");
});

test("classifyState: respects a custom critical multiplier", () => {
  assert.strictEqual(classifyState(25, 10, 3), "TRIGGERED"); // 25 < 30 (3x)
  assert.strictEqual(classifyState(31, 10, 3), "CRITICAL"); // 31 > 30 (3x)
});

// -- hasStateChanged: all 9 state-pair combinations -------------------------

const STATES = ["NORMAL", "TRIGGERED", "CRITICAL"];
const EXPECTED_FIRE = {
  "NORMAL->NORMAL": false,
  "NORMAL->TRIGGERED": true,
  "NORMAL->CRITICAL": true,
  "TRIGGERED->NORMAL": true,
  "TRIGGERED->TRIGGERED": false,
  "TRIGGERED->CRITICAL": true,
  "CRITICAL->NORMAL": true,
  "CRITICAL->TRIGGERED": false, // de-escalation: stays silent per approved design
  "CRITICAL->CRITICAL": false,
};

for (const prev of STATES) {
  for (const next of STATES) {
    const key = `${prev}->${next}`;
    test(`hasStateChanged: ${key} => fire=${EXPECTED_FIRE[key]}`, () => {
      assert.strictEqual(hasStateChanged(prev, next), EXPECTED_FIRE[key]);
    });
  }
}

// -- discountOutpaceSeverity: two ANDed thresholds -> one binding ratio -----

test("discountOutpaceSeverity: null raw input returns null", () => {
  assert.strictEqual(discountOutpaceSeverity(null), null);
});

test("discountOutpaceSeverity: missing disc_growth/sales_growth returns null", () => {
  assert.strictEqual(
    discountOutpaceSeverity({ disc_growth: null, sales_growth: 5, min_growth_threshold: 10, gap_threshold: 10 }),
    null,
  );
});

test("discountOutpaceSeverity: both growth numbers exactly at threshold => severity 1 (TRIGGERED boundary)", () => {
  // disc_growth=10 (>=min_growth_threshold=10), gap = 10-0 = 10 (>=gap_threshold=10)
  const severity = discountOutpaceSeverity({
    disc_growth: 10,
    sales_growth: 0,
    min_growth_threshold: 10,
    gap_threshold: 10,
  });
  assert.strictEqual(severity, 1);
  assert.strictEqual(classifyState(severity, 1), "NORMAL"); // strictly greater required to TRIGGER
});

test("discountOutpaceSeverity: binding constraint is the smaller ratio", () => {
  // disc_growth ratio = 20/10 = 2, gap ratio = (20-15)/10 = 0.5 -- gap is binding
  const severity = discountOutpaceSeverity({
    disc_growth: 20,
    sales_growth: 15,
    min_growth_threshold: 10,
    gap_threshold: 10,
  });
  assert.strictEqual(severity, 0.5);
});

test("discountOutpaceSeverity: both growth numbers doubled past their thresholds => CRITICAL", () => {
  // disc_growth ratio = 20/10 = 2, gap ratio = (20-0)/10 = 2 -- both exactly at 2x
  const severity = discountOutpaceSeverity({
    disc_growth: 20,
    sales_growth: 0,
    min_growth_threshold: 10,
    gap_threshold: 10,
  });
  assert.strictEqual(severity, 2);
  assert.strictEqual(classifyState(severity, 1), "TRIGGERED"); // at 2x boundary, not yet past it
  assert.strictEqual(classifyState(2.01, 1), "CRITICAL");
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
    console.log("\nAll alert-state-machine tests passed.");
  }
})();
