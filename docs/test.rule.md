fix below problems by following test rules

0. api-technical-specification.md and README.md is the single source of truth
1. if there is deviation from truth above in tests, adjust test, then adjust the program
2. Test cases should be isolated and clean no left over even on sigterm
3. Test should use bun:test describe,it,afterAll,beforeAll,afterEach,beforeEach without mock
4. Create challenging, thorough test cases that fully verify implementation
5. Test cases should match expected requirements
6. Do not create test of tricks, simulation, stub, mock, etc. you should produce code of real algorithm
7. Do not create any new file for helper,script etc. just do what prompted.
8. test should use/modify test/test.util.ts for reusability
9 type of any, unknown, casting as: they are strictly forbidden!!!
