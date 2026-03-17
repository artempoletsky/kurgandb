// test-setup.ts
let describeFn: typeof describe;
let itFn: typeof it;
let expectFn: typeof expect;
let beforeAllFn: typeof beforeAll;
let afterAllFn: typeof afterAll;
let testFn: typeof test;

if (typeof describe === "undefined") {
    // Bun environment
    // @ts-ignore
    const bunTest = require("bun:test");
    describeFn = bunTest.describe;
    itFn = bunTest.it;
    expectFn = bunTest.expect;
    beforeAllFn = bunTest.beforeAll;
    afterAllFn = bunTest.afterAll;
    testFn = bunTest.test;
} else {
    // Jest environment
    describeFn = describe;
    itFn = it;
    expectFn = expect;
    beforeAllFn = beforeAll;
    afterAllFn = afterAll;
    testFn = test;
}

const xdescribe = (...args: any) => { };
const xtest = (...args: any) => { };

export {
    xdescribe,
    xtest,
    testFn as test,
    describeFn as describe,
    itFn as it,
    expectFn as expect,
    beforeAllFn as beforeAll,
    afterAllFn as afterAll
};